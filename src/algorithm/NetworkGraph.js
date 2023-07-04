import * as fs from 'fs';
import colornames from '../styles/colors.js';

export class NetworkGraph {
    constructor() {
        this._nodes = new Map();
        this._edges = new Map();
    }

    setNode(node) {
        let n = null;
        if (this.nodes.has(node.id)) {
            n = this.nodes.get(node.id);
        } else {
            //console.count('nodes added to graph')
            n = {
                nextNodes: new Set(),
                prevNodes: new Set(),
                mesoElement: null,
                length: null,
                lngLat: null,
                locStart: null,
                locEnd: null,
                lineNationalId: null,
                chRank: null
            };
        }
        
        if (node.lngLat) n.lngLat = node.lngLat;
        if (node.locStart) n.locStart = node.locStart;
        if (node.locEnd) n.locEnd = node.locEnd;
        if (node.length) n.length = parseFloat(node.length);
        if (node.mesoElement) n.mesoElement = node.mesoElement;
        if (node.nextNode) n.nextNodes.add(node.nextNode); //{via, to}
        if (node.prevNode) n.prevNodes.add(node.prevNode); //{via, to}
        if (node.lineNationalId) n.lineNationalId = node.lineNationalId;        
        if (node.chRank) n.chRank = parseInt(node.chRank);

        this.nodes.set(node.id, n);
    }

    getEdgeWeight(id) {
        return this.edges.get(id).weight;
    }

    setEdge(edge) {
        let e = null;
        if (this.edges.has(edge.id)) {
            e = this.edges.get(edge.id);
        } else {
            e = {
                weight: 0
            }
        }

        if (edge.weight) e.weight = edge.weight;
        
        this.edges.set(edge.id, edge);
    }

    visualize(outputFile) {
        const output = fs.createWriteStream(outputFile);
        output.write("digraph {");

        let getcolor = (meso, mesoMap, colors) => {
            if (mesoMap.has(meso)) {
                return mesoMap.get(meso);
            } else {
                if (colors.length === 0) colors = [...colornames];
                let colorId = Math.floor(Math.random() * colors.length);
                let color = colors[colorId];
                colors.splice(colorId, 1);
                mesoMap.set(meso, color);
                return color;
            }
        }

        let nodes = Array.from(this.nodes.entries());//.map(([key, value]) => key, value);
        let translation = new Map();
        let colors = [...colornames];
        let mesoElements = new Map();
        for (let [key, node] of nodes) {
            let nodeId;
            if (translation.has(key)) {
                nodeId = translation.get(key);
            } else {
                nodeId = translation.size + 1;
                output.write(`"${key}" [label="${node.chRank}", style=filled, fillcolor="${getcolor(node.mesoElement, mesoElements, colors)}"];\n`)
                translation.set(key, translation.size + 1);
            }
            let prefix = `"${key}"` + " -> "
            for (let neighbour of node.nextNodes) {
                let id;
                let edge = neighbour.via;
                neighbour = neighbour.to;
                if (translation.has(neighbour)) {
                    id = translation.get(neighbour);
                } else {
                    id = translation.size + 1;
                    translation.set(neighbour, translation.size + 1);
                    if (this.nodes.has(neighbour)) output.write(`"${neighbour}" [label="${this.nodes.get(neighbour).chRank}", style=filled, fillcolor="${getcolor(this.nodes.get(neighbour).mesoElement, mesoElements, colors)}"];\n`);
                }
                let shortcut ='';
                if (/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(edge)) {
                    shortcut = 'color="red"'
                }
                output.write(prefix + `"${neighbour}" [label="${this.getEdgeWeight(edge)}" ${shortcut}]` + ";\n");
            }
        }

        output.write("\n}")
        //console.log(new Set(nodes.map(([key, node]) => node.mesoElement)));

        output.close();
        console.log(mesoElements);
    }

    visualizeClustered(outputFile) {
        const output = fs.createWriteStream(outputFile);
        output.write("digraph {\n            compound=true\n");

        let getcolor = (meso, mesoMap, colors) => {
            if (mesoMap.has(meso)) {
                return mesoMap.get(meso);
            } else {
                if (colors.length === 0) colors = [...colornames];
                let colorId = Math.floor(Math.random() * colors.length);
                let color = colors[colorId];
                colors.splice(colorId, 1);
                mesoMap.set(meso, color);
                return color;
            }
        }

        let nodes = Array.from(this.nodes.entries());//.map(([key, value]) => key, value);
        let translation = new Map();
        let colors = [...colornames];
        let mesoElements = new Map();

        let mesoGraphs = new Map(); //mesoId --> subgraphelements
        let interGrapConnnections = new Array();
        for (let [key, node] of nodes) {
            let nodeId;
            if (translation.has(key)) {
                nodeId = translation.get(key);
            } else {
                nodeId = translation.size + 1;
                let subgraph;
                if (mesoGraphs.has(node.mesoElement)) {
                    subgraph = mesoGraphs.get(node.mesoElement);                    
                } else {
                    subgraph = [];
                    mesoGraphs.set(node.mesoElement, subgraph);
                }
                subgraph.push(`"${key}" [label="${node.chRank}", style=filled, fillcolor="${getcolor(node.mesoElement, mesoElements, colors)}"];\n`);
                translation.set(key, translation.size + 1);
            }
            let prefix = `"${key}"` + " -> "
            for (let neighbour of node.nextNodes) {
                let id;
                let edge = neighbour.via;
                neighbour = neighbour.to;
                if (translation.has(neighbour)) {
                    id = translation.get(neighbour);
                } else {
                    id = translation.size + 1;
                    translation.set(neighbour, translation.size + 1);
                    if (this.nodes.has(neighbour)) {
                        let subgraph;
                        if (mesoGraphs.has(node.mesoElement)) {
                            subgraph = mesoGraphs.get(node.mesoElement);                    
                        } else {
                            subgraph = [];
                            mesoGraphs.set(this.nodes.get(neighbour).mesoElement, subgraph);
                        }
                        subgraph.push(`"${neighbour}" [label="${this.nodes.get(neighbour).chRank}", style=filled, fillcolor="${getcolor(this.nodes.get(neighbour).mesoElement, mesoElements, colors)}"];\n`);
                    }
                }
                let shortcut ='';
                if (/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(edge)) {
                    shortcut = 'color="red"'
                }
                let subgraph = interGrapConnnections;
                if (node.mesoElement === this.nodes.get(neighbour).mesoElement) {
                    subgraph = mesoGraphs.get(node.mesoElement);
                }
                subgraph.push(prefix + `"${neighbour}" [label="${this.getEdgeWeight(edge)}" ${shortcut}]` + ";\n");
            }
        }

        let clusterCount = 1;
        for (const [meso, subgraph] of mesoGraphs) {
            output.write(`  subgraph cluster_${clusterCount} {`);
            output.write(`      label="${meso}";`);

            output.write(subgraph.join(''));

            output.write("\n} \n")
            clusterCount++;
        }

        output.write(interGrapConnnections.join(''));

        output.write("\n}")
        //console.log(new Set(nodes.map(([key, node]) => node.mesoElement)));

        output.close();
        console.log(mesoElements);
    }

    get edges() {
        return this._edges;
    }

    set edges(e) {
        return this._edges = e;
    }

    get nodes() {
        return this._nodes;
    }

    set nodes(n) {
        this._nodes = n;
    }
}