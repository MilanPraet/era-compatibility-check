import EventEmitter from "events";
import TinyQueue from 'tinyqueue';
import { point } from '@turf/helpers';
import distance from '@turf/distance';
import * as N3 from 'n3';
import Utils from '../utils/Utils.js';
import { NetworkGraph } from '../algorithm/NetworkGraph.js';
import { setTimeout } from "timers/promises";
import { max } from "d3";

const NTRIPLES = 'application/n-triples';
//const SPARQL = 'http://n063-13a.wall2.ilabt.iminds.be:8890/sparql?query=';
//const SPARQL = 'http://localhost:8890/sparql?query=';
const SPARQL = `http://localhost:7200/repositories/Era-dataCH2Seperated?format=${NTRIPLES}&query=`;
//const SPARQL = `http://n073-16a.wall1.ilabt.iminds.be:7200/repositories/Era-dataCH?default-graph-uri=${GRAPH}&format=${NTRIPLES}&query=`;

export class PathFinder extends EventEmitter {
    constructor(props) {
        super();
        this._zoom = props.zoom || 10;
        this._tileCache = props.tileCache;
        this._abstTilesBaseURI = "http://127.0.0.1:3000/ldf/sparql-tiles/abstraction" //props.abstTilesBaseURI;
        this._implTilesBaseURI = "http://127.0.0.1:3000/ldf/sparql-tiles/implementation" //props.implTilesBaseURI;
        this._fetch = props.fetch || null;
        this._debug = props.debug;
    }

    // Yen's algorithm for top-k paths (https://en.wikipedia.org/wiki/Yen%27s_algorithm).
    async yen({ from, to, K, NG }) {
        // Top-k paths
        const ksp = [];
        // Run A* to find first path
        const sp = await this.aStar({ from, to, NG });
        if (this.debug) console.debug('DEBUG: FIRST SHORTEST PATH', sp);

        if (sp) {
            ksp.push(sp);

            for (let k = 1; k < K; k++) {
                // Heap that will hold the new alternative paths
                if (this.debug) console.debug('DEBUG: K = ', k);
                const prevPath = ksp[k - 1];
                if (!prevPath) break;
                let nextShortestPath = null;
                if (this.debug) console.debug('DEBUG: -------------------------------------');

                // Iterate over the nodes of the previous found path
                for (let i = 0; i < prevPath.nodes.length - 2; i++) {
                    // Create a clone of the network graph that can be altered to allow finding new shortest paths
                    const NGClone = new NetworkGraph();
                    NGClone.nodes = Utils.deepClone(NG.nodes);
                    // Node from which an alternative route will be calculated
                    const spurNode = NGClone.nodes.get(prevPath.nodes[i].id);
                    if (this.debug) console.debug('DEBUG: SPUR NODE', spurNode);

                    // The sequence of nodes that lead to the spur node
                    const rootPath = { nodes: Utils.deepClone(prevPath.nodes).slice(0, i) };
                    if (this.debug) console.debug('DEBUG: ROOT PATH', rootPath);

                    // Go over all the, so far, found shortest paths
                    // and remove the link to the node following the spur node if a path matches the root path.
                    ksp.forEach(p => {
                        if (this.pathsAreEqual(rootPath.nodes, p.nodes.slice(0, i))) {
                            spurNode.nextNodes.delete(p.nodes[i + 1].id);
                        }
                    });

                    // Remove all the root path nodes from the cloned network graph
                    rootPath.nodes.forEach(n => {
                        NGClone.nodes.delete(n.id);
                    });
                    // Remove link to first SoL-related net element to avoid calculating the same shortest path as before
                    if (i === 0) NGClone.nodes.delete(prevPath.nodes[1].id);

                    // If spur node is FROM add all possible micro net elements except the previously used one
                    const microNEs = i === 0 ? from.microNEs.filter(p => p !== prevPath.nodes[0].id)
                        : [prevPath.nodes[i].id];

                    // Calculate new path with A* from spur to destination
                    const spurPath = await this.aStar({
                        from: {
                            microNEs: microNEs,
                            lngLat: spurNode.lngLat,
                            length: spurNode.length || 0
                        }, to, NG: NGClone
                    });

                    if (spurPath) {
                        const newPath = { nodes: rootPath.nodes.concat(spurPath.nodes) };
                        if (this.debug) console.debug('DEBUG: FOUND PATH', newPath);

                        // Check that the spur path does not exist already
                        if (!this.containsPath(newPath, ksp)) {
                            // Measure new path's length and add it to the heap
                            newPath.length = this.calculatePathLength(newPath);

                            if (newPath.length > prevPath.length) {
                                if (this.debug) console.debug('DEBUG: NEW PATH', newPath);
                                // Force the new path finding for performance
                                nextShortestPath = newPath;
                                break;
                            }
                        }
                    }
                }

                if (nextShortestPath) {
                    if (this.debug) console.debug('DEBUG: NEW SHORTEST PATH', nextShortestPath);
                    ksp.push(nextShortestPath);
                }
            }


        }

        return ksp;
    }

    async bidirectionalaStar({from, to, NG, perf}) {    
        let forwardSearch = this.biaStarHalf({from: from, to: to, NG, reverse: false, perf});        
        let backwardSearch = this.biaStarHalf({from: to, to: from, NG, reverse: true, perf});

        let forwardBuffer = await forwardSearch.next();            
        let backwardBuffer = await backwardSearch.next();

        let midpoint

        let forwardHit = 0;
        let backwardHit = 0;

        for (let el of forwardBuffer.value.explored) {
            if (backwardBuffer.value.explored.has(el)) {
                //console.log(forwardBuffer.value.explored);
                //console.log(backwardBuffer.value.explored);
                midpoint = el;
                break;
            }
        }  

        // allow to execcute both directions without both being blocked by tile fetches
        let forward = new Promise(resolve => {
            forwardSearch.next().then(value => resolve({ i: 0, val: value }));
            console.log('forwardHit: ' + ++forwardHit);
        });
        let backward = new Promise(resolve => {
            backwardSearch.next().then(value => resolve({ i: 1, val: value }));
            console.log('backwardHit: ' + ++backwardHit);
        });

        let buffers = [
            forward,
            backward
        ];
        let first = 0;
        while (midpoint === undefined) {
            // could be buffered and changed to promise.any
            let { i , val } = await Promise.race(buffers); // does not switch direction that often
            //let { i, val } = await buffers[first % buffers.length]; // forces direction switch every time

            // add new promise to buffer
            if (i === 0) {
                if (!(val.done)){                    
                    buffers[i] = new Promise(resolve => {                        
                        //console.log('forwardHit: ' + ++forwardHit);
                        forwardSearch.next().then(value => resolve({ i: 0, val: value }));
                    });
                    forwardBuffer = val;
                } else {
                    buffers[i] = buffers[1];
                    buffers.pop();
                }
            } else {
                if (!(val.done)) {
                    buffers[i % buffers.length] = new Promise(resolve => {                        
                        //console.log('backwardHit: ' + ++backwardHit);
                        backwardSearch.next().then(value => resolve({ i: 1, val: value }));
                    });
                    backwardBuffer = val;
                } else buffers.pop();
            }

            // find intersection point of set
            for (const el of forwardBuffer.value.pathMap.keys()) {
                for (const back of backwardBuffer.value.pathMap.keys()) {
                    if (el === back) midpoint = el;
                }
            }
            /*for (let el of forwardBuffer.value.explored) {
                if (backwardBuffer.value.explored.has(el)) {
                    midpoint = el;
                }
            }*/

            if (buffers.length === 0 && !midpoint) {
                return null
            }
            first++;
        }
        
        // no path was found
        //if(!midpoint) return null;

        // get correct midpoint node to be able to reconstruct the path
        for (let key of forwardBuffer.value.pathMap.keys()) {
            if (backwardBuffer.value.pathMap.has(key)) {
                midpoint = key;
                break;
            }
        }

        let partA = forwardBuffer.value.rebuildPath(midpoint);
        let partB = backwardBuffer.value.rebuildPath(midpoint);

        // relax shortcuts TODO

        // 1. send query using found edges as values
        // 2. sparql property paths will relax all shortcuts of these edges
        // 2.1 might need to recover netRelations ids from pathmap
        // 3. reorder unordered sparql results and return path
        partA.nodes = partA.nodes.concat(partB.nodes.slice(0, -1).reverse());
        partA.length = partA.length - partB.nodes.at(-1).length + partB.length;
        if (perf) perf.explored = forwardBuffer.value.explored.size + backwardBuffer.value.explored.size;

        // relax edges
        const shortcuts = [];
        const shortcutIndex = [];
        let i = 0;
        for (const node of partA.nodes) {
            // test if netRelation is a shortcut
            if (/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(node.via)) {
                console.log("WE HAVE PROCESSED A SHORTCUT");
                shortcuts.push(node);
                shortcutIndex.push(i);
            }
            i += 1;
        }

        // relaxation query
        const query = `PREFIX era: <http://data.europa.eu/949/>
        construct {
            ?path era:elementA ?elA ;
                  era:elementB ?elB ;
                  era:compoundWeight ?weight .
            
        }  where { 
            VALUES ?origPath {
                ${shortcuts.map(e => `<${e.via}>`).join('\n')}
            }
            ?origPath (era:skipEdge1|era:skipEdge2)+ ?path .
            
            ?path era:elementA ?elA;
                     era:elementB ?elB;
                  era:compoundWeight ?weight .
            filter not exists { ?path era:skipEdge1 ?skip}
        }`;

        const opts = { headers: { Accept: NTRIPLES } };
        const body = await (await fetch(SPARQL + encodeURIComponent(query), opts)).text();
        const quads = new N3.Parser({ format: 'N-Triples' })
            .parse(await (await fetch(SPARQL + encodeURIComponent(query), opts)).text());
        let netRel = new Map() // [elA, elB]
        for (let quad of quads) {
            if (quad.predicate.value === 'http://data.europa.eu/949/elementA') {
                if (!netRel.has(quad.subject.value)) {
                    netRel.set(quad.subject.value, [quad.object.value, undefined]);
                } else {
                    netRel.get(quad.subject.value)[0] = quad.object.value;
                }
            } else if (quad.predicate.value === 'http://data.europa.eu/949/elementB') {
                if (!netRel.has(quad.subject.value)) {
                    netRel.set(quad.subject.value, [undefined, quad.object.value]);
                } else {
                    netRel.get(quad.subject.value)[1] = quad.object.value;
                }
            }
        }        
        let chains = [];
        for (const node of shortcuts) {
            // find first pair in fetched list
            const relaxedShortcut = [];
            let rShLen = -1;
            let searchEl = node.id;
            while (rShLen != relaxedShortcut.length) {
                rShLen = relaxedShortcut.length;
                for (const [rel, el] of netRel.entries()) {
                    let index = el.indexOf(searchEl);
                    if (index != -1) {
                        relaxedShortcut.push({id: el[index], via: rel});
                        searchEl = el[(index  + 1) % 2];
                        netRel.delete(rel);
                        continue;
                    }
                }
            }
            chains.push(relaxedShortcut);
        }

        // replace in reverse order to avoid recalculating insert indexes
        i = -1;
        for (const index of shortcutIndex.reverse()) {
            partA.nodes.splice(index, 0, ...(chains.at(i)))
            i -= 1;
        }        

        return partA;
    }

    async *biaStarHalf({ from, to, NG, reverse }) {
        if (this.debug) console.debug('DEBUG: FROM: ', from);
        if (this.debug) console.debug('DEBUG: TO: ', to);

        // Nodes distance map
        const pathMap = new Map();
        // All the possible departure micro NetElements
        const fromSet = new Set(from.microNEs);
        // All the possible arrival micro NetElements
        const toSet = new Set(to.microNEs);
        // Set to store visited nodes
        const explored = new Set();
        // Set to avoid adding the same Node to the queue more than once 
        const queued = new Set();
        // Priority queue
        const queue = new TinyQueue([], (a, b) => { return a.cost - b.cost });

        // Add all starting NetElements to the queue with initial metrics
        const initDist = from.lngLat ? distance(point(from.lngLat), point(to.lngLat)) : null;

        for (const f of from.microNEs) {
            queue.push({
                id: f,
                distance: initDist,
                length: NG.nodes.get(f).length || 0,
                cost: 0
            });
            queued.add(f);
        }

        // In this variable we will store the found arrival NetElement
        let dest = null;

        let rebuildPath = (dest) => {
            // Rebuild path
            if (this.debug) console.debug('DEGUB: Resulting PathMap', pathMap);
            let totLength = 0;
            let node = pathMap.get(dest);
            const path = { nodes: [{ id: dest, length: node.length, lngLat: node.lngLat, rank: NG.nodes.get(dest).chRank, via: node.via  }] };
            totLength += node.length;

            while (!fromSet.has(node.from)) {
                const prevNode = { id: node.from };

                // Get previous pathMap node
                node = pathMap.get(node.from);
                prevNode.length = node.length;
                prevNode.lngLat = node.lngLat;
                prevNode.rank = NG.nodes.get(node.from).chRank;
                prevNode.via = node.via

                path.nodes.unshift(prevNode);
                totLength += node.length;
            }
            path.nodes.unshift({ id: node.from, length: NG.nodes.get(node.from).length, lngLat: from.lngLat, rank: NG.nodes.get(node.from).chRank, via: node.via });
            path.length = totLength;

            if (this.debug) console.debug('DEBUG: PATH: ', path);

            return path;
        }
        let maxRank = 0;

        while (queue.length) {
            const here = queue.pop();
            if (this.debug) console.debug('DEBUG: HERE: ', here);

            // Arrived at destination!!
            if (toSet.has(here.id)) {
                dest = here.id;
                explored.add(here.id);
                break;
            };

            const hereNode = NG.nodes.get(here.id);
            if (this.debug) console.debug('DEBUG: HERE\'s node: ', hereNode);

            // Add micro net elements to visited list
            explored.add(here.id);

            // Skip if no there are no outgoing edges from this node, it means it is a dead end
            let followUp = reverse ? hereNode.prevNodes : hereNode.nextNodes;
            if (followUp.size > 0) {
                // Iterate over the next reachable nodes
                for (const [i, n] of followUp.entries()) {
                    const next = { id: n.to };
                    let nextNode = NG.nodes.get(next.id);

                    if (this.debug) {
                        console.debug('DEBUG: NEXT: ', next);
                        console.debug('DEBUG: NEXT node: ', nextNode);
                    };



                    // If undefined it means it was deliberately removed to find alternative paths, so skip it
                    if (!nextNode) continue;
                    // Skip if previously explored
                    if (explored.has(next.id)) continue;

                    //if (Array.from(followUp).some(iri => /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(iri.via))) {
                    //    console.log("this node has a shortcut");
                    //}
                    //if (!reverse && hereNode.chRank === 586956) console.log("THE MIDPOINT HAS BEEN FOUND!!!!!!!!!!!!!!!!!!!")

                    // skip if lower in hierarchy
                    //if(hereNode.chRank === undefined) console.log( `NODE ${hereNode} DOES NOT HAVE A CHRANK PROPERTY`)
                    //if (hereNode.chRank > nextNode.chRank) continue;
                    /*if (hereNode.chRank > maxRank) {
                        maxRank = hereNode.chRank;
                        console.log(`${reverse ? 'backward max: ' : 'forward max: '} ${maxRank}`);
                    }*/

                    /**
                     * Is possible this next node belongs to a tile we haven't fetched yet.
                     * We know it is so if the next node does not have next reachable nodes,
                     * or does not have values for neither length and geolocation
                     * or because its tile is not in the cache.
                     * So get on it and fetch the tile!
                    */


                    // problems when fetching SoL to SoL
                    // try find out when this is beig followed
                    // if true fetch both opEnd and opStart tiles
                    
                    if ((nextNode.nextNodes.size === 0) || (!nextNode.mesoElement)
                        || (nextNode.lngLat 
                            && !this.tileCache.has(`${this.abstTilesBaseURI}/${this.zoom}/${Utils.longLat2Tile(nextNode.lngLat, this.zoom)}`))) {
                        if((!(nextNode.lngLat)) && !(nextNode.locStart)) continue;
                        //const tileId = `${this.abstTilesBaseURI}/${this.zoom}/${Utils.longLat2Tile(nextNode.lngLat, this.zoom)}`;
                        //if (this.debug) console.debug(`DEBUG: FETCHING TILE ${tileId} for node: \n`, next.id);
                        nextNode = await this.getMissingTile(next.id, nextNode, NG);
                        if (this.debug) console.debug('DEBUG: FETCHED NEXT node: ', nextNode);
                        // If no nextNode is returned it means we reached the end of the line
                        if (!nextNode) continue;
                    }

                    /**
                    * Calculate the accumulated cost of this potential next node 
                    * based on its length and distance to the destination.
                    * First get the Haversine distance to the destination. If not possible to calculate 
                    * (because NetElement is from a SoL and does not have a geolocation) 
                    * then get the geolocation of the next node that belongs to an OP.
                    * Fallback to use the distance of the current node.
                    */
                    let geoDist = null;
                    if (nextNode.lngLat) {
                        // Next node has geolocation
                        geoDist = distance(point(nextNode.lngLat), point(to.lngLat));
                    } else {
                        let nn = nextNode;
                        let hereId = here.id;
                        let nextId = next.id;

                        // Find the next node with geolocation. Consider the case where more than one consecutive
                        // micro NetElements exist within a SoL.
                        while (geoDist === null) {
                            // Make sure to avoid reverse edges to prevent infinite loops
                            const nextNextNodeId =  this.getValidNextNode(hereId, nn, reverse);
                            const nextNextNode = NG.nodes.get(nextNextNodeId);
                            if (this.debug) console.debug('DEBUG: NEXT NEXT node', nextNextNode);
                            if (nextNextNode) {
                                const nextNextFollowUp = reverse ? nextNextNode.prevNodes : nextNextNode.nextNodes;
                                if (nextNextNode.lngLat) {
                                    geoDist = distance(point(nextNextNode.lngLat), point(to.lngLat));
                                } else if (nextNextFollowUp.size > 0) {
                                    hereId = nextId;
                                    nextId = nextNextNodeId;
                                    nn = nextNextNode;
                                } else {
                                    // We reached a tile limit. 
                                    // Use the geolocation of the current node as approximate reference.
                                    geoDist = here.distance;
                                }
                            } else {
                                // We reached the end of a line. 
                                // Use the geolocation of the current node as approximate reference.
                                geoDist = here.distance;
                            }
                        }
                    }

                    // Get NetElement length if given otherwise use the latest known length from previous nodes.
                    // Use a factor of 10 on the length to allow the geo distance heuristic to influence the queue.
                    const length = nextNode.length / 10 || 0;
                    if (this.debug) console.debug(`GEO-DISTANCE: ${geoDist}`);
                    if (this.debug) console.debug(`TRACK LENGTH: ${length}`);

                    // Assign cost of next node as the sum of above metrics plus the cost accumulated so far to get here.
                    let contracted = 0;
                    if (NG.edges.has(n.via)) contracted = NG.edges.get(n.via);
                    next.cost = geoDist + length + here.cost + contracted.weight;

                    // Register next node metrics
                    next.distance = geoDist;
                    next.length = length;

                    // Add to the path map if it's a shorter route or a newly discovered node
                    if (pathMap.has(next.id)) {
                        if (next.cost < pathMap.get(next.id).cost) {
                            pathMap.set(next.id, {
                                from: here.id,
                                via: n.via,
                                cost: next.cost,
                                length: nextNode.length || 0,
                                lngLat: nextNode.lngLat
                            });
                            if (this.debug) console.debug('DEBUG: PathMap set: ', next.id, here.id, next.cost);
                        }
                    } else {
                        pathMap.set(next.id, {
                            from: here.id,
                            via: n.via,
                            cost: next.cost,
                            length: nextNode.length || 0,
                            lngLat: nextNode.lngLat
                        });
                        if (this.debug) console.debug('DEBUG: PathMap set: ', next.id, here.id, next.cost);
                    }

                    // Add to the queue
                    if (!queued.has(next.id)) {
                        queue.push(next);
                        queued.add(next.id);
                        if (this.debug) console.debug('DEBUG: Queued: ', next);
                    }
                }
                if (this.debug) console.debug('DEBUG: *************************************');
            }
            yield { explored, pathMap, rebuildPath };
        }
        return { explored, pathMap, rebuildPath };

        /*if (dest) {
            return rebuildPath(dest);
        } else {
            // We didn't find a route :(
            return null;
        }*/
    }

    async aStar({ from, to, NG }) {
        if (this.debug) console.debug('DEBUG: FROM: ', from);
        if (this.debug) console.debug('DEBUG: TO: ', to);

        // Nodes distance map
        const pathMap = new Map();
        // All the possible departure micro NetElements
        const fromSet = new Set(from.microNEs);
        // All the possible arrival micro NetElements
        const toSet = new Set(to.microNEs);
        // Set to store visited nodes
        const explored = new Set();
        // Set to avoid adding the same Node to the queue more than once 
        const queued = new Set();
        // Priority queue
        const queue = new TinyQueue([], (a, b) => { return a.cost - b.cost });

        // Add all starting NetElements to the queue with initial metrics
        const initDist = from.lngLat ? distance(point(from.lngLat), point(to.lngLat)) : null;

        for (const f of from.microNEs) {
            queue.push({
                id: f,
                distance: initDist,
                length: from.length || 0,
                cost: 0
            });
            queued.add(f);
        }

        // In this variable we will store the found arrival NetElement
        let dest = null;

        while (queue.length) {
            const here = queue.pop();
            if (this.debug) console.debug('DEBUG: HERE: ', here);

            // Arrived at destination
            if (toSet.has(here.id)) {
                dest = here.id;
                break;
            };

            const hereNode = NG.nodes.get(here.id);
            if (this.debug) console.debug('DEBUG: HERE\'s node: ', hereNode);

            // Add micro and meso nodes to visited list
            explored.add(here.id);
            explored.add(hereNode.mesoElement)

            // Skip if no there are no outgoing edges from this node, it means it is a dead end
            if (hereNode.nextNodes.size > 0) {
                // Iterate over the next reachable nodes
                for (const [i, n] of hereNode.nextNodes.entries()) {
                    const next = { id: n };
                    let nextNode = NG.nodes.get(next.id);

                    if (this.debug) {
                        console.debug('DEBUG: NEXT: ', next);
                        console.debug('DEBUG: NEXT node: ', nextNode);
                    };

                    // If undefined it means it was deliberately removed to find alternative paths, so skip it
                    if (!nextNode) continue;
                    // Skip if previously explored
                    if (explored.has(next.id) || explored.has(nextNode.mesoElement)) continue;

                    /**
                     * Is possible this next node belongs to a tile we haven't fetched yet.
                     * We know it is so if the next node does not have next reachable nodes,
                     * or does not have values for neither length and geolocation
                     * or because its tile is not in the cache.
                     * So get on it and fetch the tile!
                    */

                    if (nextNode.nextNodes.size === 0 || (nextNode.lngLat 
                        && !this.tileCache.has(`${this.tilesBaseURI}/${this.zoom}/${Utils.longLat2Tile(nextNode.lngLat, this.zoom)}`))) {
                        if (this.debug) console.debug('DEBUG: FETCHING TILE for node: ', next.id);
                        nextNode = await this.getMissingTile(next.id, nextNode, NG);
                        if (this.debug) console.debug('DEBUG: FETCHED NEXT node: ', nextNode);
                        // If no nextNode is returned it means we reached the end of the line
                        if (!nextNode) continue;
                    }

                    /**
                    * Calculate the accumulated cost of this potential next node 
                    * based on its length and distance to the destination.
                    * First get the Haversine distance to the destination. If not possible to calculate 
                    * (because NetElement is from a SoL and does not have a geolocation) 
                    * then get the geolocation of the next node that belongs to an OP.
                    * Fallback to use the distance of the current node.
                    */
                    let geoDist = null;
                    if (nextNode.lngLat) {
                        // Next node has geolocation
                        geoDist = distance(point(nextNode.lngLat), point(to.lngLat));
                    } else {
                        let nn = nextNode;
                        let hereId = here.id;
                        let nextId = next.id;

                        // Find the next node with geolocation. Consider the case where more than one consecutive
                        // micro NetElements exist within a SoL.
                        while (geoDist === null) {
                            // Make sure to avoid reverse edges to prevent infinite loops
                            const nextNextNodeId = this.getValidNextNode(hereId, nn);
                            const nextNextNode = NG.nodes.get(nextNextNodeId);
                            if (this.debug) console.debug('DEBUG: NEXT NEXT node', nextNextNode);
                            if (nextNextNode) {
                                if (nextNextNode.lngLat) {
                                    geoDist = distance(point(nextNextNode.lngLat), point(to.lngLat));
                                } else if (nextNextNode.depEdges.size > 0) {
                                    hereId = nextId;
                                    nextId = nextNextNodeId;
                                    nn = nextNextNode;
                                } else {
                                    // We reached a tile limit. 
                                    // Use the geolocation of the current node as approximate reference.
                                    geoDist = here.distance;
                                }
                            } else {
                                // We reached the end of a line. 
                                // Use the geolocation of the current node as approximate reference.
                                geoDist = here.distance;
                            }
                        }
                    }

                    // Get NetElement length if given otherwise use the latest known length from previous nodes.
                    // Use a factor of 10 on the length to allow the geo distance heuristic to influence the queue.
                    const length = nextNode.length / 10 || 0;
                    if (this.debug) console.debug(`GEO-DISTANCE: ${geoDist}`);
                    if (this.debug) console.debug(`TRACK LENGTH: ${length}`);

                    // Assign cost of next node as the sum of above metrics plus the cost accumulated so far to get here.
                    next.cost = geoDist + length + here.cost;

                    // Register next node metrics
                    next.distance = geoDist;
                    next.length = length;

                    // Add to the path map if it's a shorter route or a newly discovered node
                    if (pathMap.has(next.id)) {
                        if (next.cost < pathMap.get(next.id).cost) {
                            pathMap.set(next.id, {
                                from: here.id,
                                cost: next.cost,
                                length: nextNode.length || 0,
                                lngLat: nextNode.lngLat
                            });
                            if (this.debug) console.debug('DEBUG: PathMap set: ', next.id, here.id, next.cost);
                        }
                    } else {
                        pathMap.set(next.id, {
                            from: here.id,
                            cost: next.cost,
                            length: nextNode.length || 0,
                            lngLat: nextNode.lngLat
                        });
                        if (this.debug) console.debug('DEBUG: PathMap set: ', next.id, here.id, next.cost);
                    }

                    // Add to the queue
                    if (!queued.has(next.id)) {
                        queue.push(next);
                        queued.add(next.id);
                        if (this.debug) console.debug('DEBUG: Queued: ', next);
                    }
                }
                if (this.debug) console.debug('DEBUG: *************************************');
            }
        }

        if (dest) {
            // Rebuild path
            if (this.debug) console.debug('DEGUB: Resulting PathMap', pathMap);
            let totLength = 0;
            let node = pathMap.get(dest);
            const path = { nodes: [{ id: dest, length: node.length, lngLat: node.lngLat }] };
            totLength += node.length;

            while (!fromSet.has(node.from)) {
                const prevNode = { id: node.from };

                // Get previous pathMap node
                node = pathMap.get(node.from);
                prevNode.length = node.length;
                prevNode.lngLat = node.lngLat;

                path.nodes.unshift(prevNode);
                totLength += node.length;
            }
            path.nodes.unshift({ id: node.from, length: from.length, lngLat: from.lngLat });
            path.length = totLength;

            if (this.debug) console.debug('DEBUG: PATH: ', path);

            return path;
        } else {
            // We didn't find a route :(
            return null;
        }
    }

    async getMissingTile(mne, node, NG) {
        let coords;
        if (node.locStart) coords = [node.locStart, node.locEnd];
        if (coords === undefined) coords = [node.lngLat];
        if (coords) {
            //await this.fetchImplementationTile({ coords, NG });
            //console.log(node);
            //await this.fetchAbstractionTile({ coords, NG, rank: node.chRank });
            for (const coord of coords) {
                await this.fetchAbstractionTile({ coord, NG });
            }
            //await this.fetchAbstractionTile({ coords, NG });
            node = NG.nodes.get(mne);

            // Signal that a tile has been fetched
            this.emit('tile', { coords });
            return node;
        } else {
            throw new Error(`No geo coordinates found for ${mne}`);
        }
    }

    async fetchImplementationTile({ coords, NG }) {
        const tileUrl = `${this.implTilesBaseURI}/${this.zoom}/${Utils.longLat2Tile(coords, this.zoom)}`
        let res = null;

        if (this.fetch) {
            res = await this.fetch(tileUrl, { headers: { 'Accept': 'application/n-triples' } });
        } else {
            res = await fetch(tileUrl, { headers: { 'Accept': 'application/n-triples' } });
        }

        const rdfParser = N3.Parser ? new N3.Parser({ format: 'N-Triples' })
            : new N3.default.Parser({ format: 'N-Triples' });

        const quads = rdfParser.parse(await res.text());
        NG.tripleStore.addAll(quads);
    }

    async fetchAbstractionTile({ coord, NG, rank }) {
        const tileUrl = `${this.abstTilesBaseURI}/${this.zoom}/${Utils.longLat2Tile(coord, this.zoom)}`///${rank}
        console.log("fetching next tile:" + tileUrl);
        let res = null;

        if (this.fetch) {
            res = await this.fetch(tileUrl, { headers: { 'Accept': 'application/n-triples' } });
        } else {
            res = await fetch(tileUrl, { headers: { 'Accept': 'application/n-triples' } });
        }

        // Register fetched tile in the cache
        this.tileCache.add(tileUrl);

        const rdfParser = N3.Parser ? new N3.Parser({ format: 'N-Triples' })
            : new N3.default.Parser({ format: 'N-Triples' });

        const quads = rdfParser.parse(await res.text());
        
        //NG.tripleStore.addAll(quads);
        Utils.processTopologyQuads(quads, NG)
        //check for query that returns nodes without coordinates
        /*for (let node of NG.nodes.keys()) {
            if (NG.nodes.get(node).lngLat === undefined) {
                console.debug(`FAILURE ${tileUrl} returns nodes without coordinates`);
                //console.log(`node: ${node} does not have a geolaction fetched`);
            }
        }*/
    }

    getValidNextNode(hereId, node, reverse = false) {
        let followUp = reverse ? node.prevNodes : node.nextNodes;
        for (const nn of followUp.values()) {
            if (nn.to !== hereId) {
                return nn.to;
            }
        }
    }

    pathsAreEqual(p1, p2) {
        let i = p1.length;
        while (i--) {
            if (p1[i] && p2[i] && p1[i].id !== p2[i].id) return false;
        }
        return true;
    }

    containsPath(p, paths) {
        for (const path of paths) {
            if (this.pathsAreEqual(p.nodes, path.nodes)) {
                return true;
            }
        }
        return false;
    }

    calculatePathLength(p) {
        let length = 0;
        for (const node of p.nodes) {
            if (node.length) length += node.length;
        }
        return length;
    }

    get zoom() {
        return this._zoom;
    }

    get abstTilesBaseURI() {
        return this._abstTilesBaseURI;
    }

    get implTilesBaseURI() {
        return this._implTilesBaseURI;
    }

    get tileCache() {
        return this._tileCache;
    }

    get fetch() {
        return this._fetch;
    }

    get debug() {
        return this._debug;
    }
}