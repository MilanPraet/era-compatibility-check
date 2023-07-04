import commander from 'commander';
import GraphStore from '@graphy/memory.dataset.fast';
import { NetworkGraph } from '../src/algorithm/NetworkGraph.js';
import { PathFinder } from '../src/algorithm/PathfinderNew.js';
import undici from 'undici';
import N3 from "n3";
import Utils from '../src/utils/Utils.js';
import { ERA } from '../src/utils/NameSpaces.js';
import {
    IMPLEMENTATION_TILES,
    ABSTRACTION_TILES,
    ABSTRACTION_ZOOM
} from '../src/config/config.js';
import {parse} from 'csv-parse';
import fs from 'fs'
import path from 'path';
import { createGunzip } from "zlib";
import readline from "readline";
import jsonlParser from "stream-json/jsonl/Parser.js";
import { delimiter } from 'path';
import {stringify} from 'csv-stringify';
import { resourceLimits } from 'worker_threads';
import colornames from '../src/styles/colors.js';

const program = new commander.Command();

program
    .option('--from <from>', 'origin OP URI')
    .option('--to <to>', 'destination OP URI')
    .option('--file <file>', 'file with test routes')
    .option('--zoom <zoom>', 'zoom level for topology tile fetching')
    .option('--export <export>', 'filename with results');

program.parse(process.argv);

const GRAPH = 'http://era.europa.eu/knowledge-graph'
const NTRIPLES = 'application/n-triples';
//const SPARQL = 'http://n063-13a.wall2.ilabt.iminds.be:8890/sparql?query=';
//const SPARQL = 'http://localhost:8890/sparql?query=';
const SPARQL = `http://localhost:7200/repositories/Era-dataCH2Seperated?format=${NTRIPLES}&query=`;
//const SPARQL = `http://n073-16a.wall1.ilabt.iminds.be:7200/repositories/Era-dataCH?default-graph-uri=${GRAPH}&format=${NTRIPLES}&query=`;
//const SPARQL = 

// Logging level
const debug = 0;//program.debug;
// Topology zoom
const tz = 10 //ABSTRACTION_ZOOM; //12 most of times smallest to fetch region
// Init KG store
let graphStore = GraphStore();
// Init Network Graph
let NG = new NetworkGraph();
NG.tripleStore = graphStore;
// Tile cache
let tileCache = new Set();

async function testRoute(fromId, toId, perf) {

    let prepT0 = performance.now();

    //const fromId = program.from;
    //const toId = program.to;

    // Get geolocations of OPs
    await fetchOPLocation([fromId, toId]);

    const fromCoords = Utils.getCoordsFromOP(fromId, graphStore);
    if (!fromCoords) {
        console.error(`ERROR: Couldn't find location of provided FROM OP ${fromId}`);
        return;
    }

    const toCoords = Utils.getCoordsFromOP(toId, graphStore);
    if (!toCoords) {
        console.error(`ERROR: Couldn't find location of provided FROM OP ${toId}`);
        return;
    }

    // Fetch initial data tiles
    await Promise.all([
        fetchTileSet(fromCoords),
        fetchTileSet(toCoords)
    ]);

    // FROM and TO objects
    const from = {};
    const to = {};

    // Get reachable micro NetElements of FROM Operational Point
    //let fromMicroNEs =
    let test = Utils.getMicroNetElements(fromId, graphStore);
    let fromMicroNEs = test.filter(ne => NG.nodes.has(ne.value))
    fromMicroNEs = fromMicroNEs.map(ne => ne.value);;
    const fromOp = Utils.getOPInfo(fromId, graphStore);
    const fromLabel = Utils.getLiteralInLanguage(fromOp[ERA.opName], 'en');

    if (fromMicroNEs.length === 0) {
        // Show warning of disconnected NetElement
        console.error(`ERROR: Operational Point ${fromLabel} is not connected to the rail network`);
        return;
    }

    // Set FROM OP parameters
    from.microNEs = fromMicroNEs;
    from.lngLat = fromCoords;
    from.length = 0

    // Get reachable micro NetElements of FROM Operational Point
    const toMicroNEs = Utils.getMicroNetElements(toId, graphStore)
        .filter(ne => NG.nodes.has(ne.value))
        .map(ne => ne.value);;
    const toOp = Utils.getOPInfo(toId, graphStore);
    const toLabel = Utils.getLiteralInLanguage(toOp[ERA.opName], 'en');

    if (toMicroNEs.length === 0) {
        // Show warning of disconnected NetElement
        console.error(`ERROR: Operational Point ${toLabel} is not connected to the rail network`);
        return;
    }

    // Set TO OP parameters
    to.microNEs = toMicroNEs;
    to.lngLat = toCoords;
    to.length = 0

    console.info(`INFO: Calculating shortest path between ${fromLabel} and ${toLabel}...`);
    const pathFinder = new PathFinder({
        tileCache,
        tilesBaseURI: ABSTRACTION_TILES,
        zoom: tz,
        fetch,
        debug
    });
    /*const planner = new NBAStar({
        NG,        
        zoom: tz,
        tilesBaseURL:   ABSTRACTION_TILES,
        distance: Utils.harvesineDistance,
        heuristic: Utils.euclideanDistance
    })*/

    perf.preparation = performance.now() - prepT0; 

    const t0 = performance.now();
    // Calculate route
    const path = await pathFinder.bidirectionalaStar({ from, to, NG, perf, debug });
    //const path = await planner.findPath(from, to);
    const t1 = performance.now();
    //console.info(`INFO: Found route: `, JSON.stringify(path, null, 3));
    //console.info('Route caluclated in', t1 - t0, 'ms');

    perf.totalTime = t1 - t0;
    perf.tiles = tileCache.size;

    return path
}

    async function fetch(url, opts) {
        opts.bodyTimeout = 600000
        opts.headersTimeout = 600000
    const { body } = await undici.request(url, opts);
    console.log(tileCache.size);
    return body;
}

async function fetchOPLocation(ops) {
    console.info('INFO: Fetching Operational Points geolocation...');

    const values = ops.map((op, i) => {
        return `(<${op}>)`;
    }).join('\n');

    const query = `PREFIX wgs: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
    PREFIX era: <http://data.europa.eu/949/>
    CONSTRUCT WHERE {
        ?op wgs:location ?loc_0.
        ?loc_0 geosparql:asWKT ?wkt_0.
        VALUES (?op) { #when using graphdb instead of virtuoso
            ${values}
        }
    }`;

    /*
        FILTER(?op = <${ops[0]}> || ?op = <${ops[1]}>)
        */

    const opts = { headers: { Accept: NTRIPLES }};
    let triples;
    try {
    triples = await fetch(SPARQL + encodeURIComponent(query), opts);
    triples = await triples.text();
    const quads = new N3.Parser({ format: 'N-Triples' })
        .parse(triples);
    graphStore.addAll(quads);
    }
    catch (e) {
        console.log("location fetching FAILED");
    }
}

async function fetchTileSet(coords) {
    await Promise.all([
        fetchImplTile({ coords }),
        fetchAbsTile({ coords })
    ]);
}

async function fetchImplTile({ coords }) {
    const tileUrl = `${IMPLEMENTATION_TILES}/${15}/${Utils.long2Tile(coords[0], 15)}/${Utils.lat2Tile(coords[1], 15)}`;
    if (!tileCache.has(tileUrl)) {
        tileCache.add(tileUrl);
        console.info('INFO: Fetching tile ', tileUrl);
        const quads = new N3.Parser({ format: 'N-triples' })
            .parse(await (await fetch(tileUrl, { headers: { Accept: NTRIPLES } })).text());
        graphStore.addAll(quads);
    }
}

async function fetchAbsTile({ coords }) {
    const tileUrl = `${ABSTRACTION_TILES}/${tz}/${Utils.long2Tile(coords[0], tz)}/${Utils.lat2Tile(coords[1], tz)}`;
    if (!tileCache.has(tileUrl)) {
        tileCache.add(tileUrl);
        console.info('INFO: Fetching tile ', tileUrl);
        const before = NG.nodes.size;
        const quads = new N3.Parser({ format: 'N-triples' })
            .parse(await (await fetch(tileUrl, { headers: { Accept: NTRIPLES } })).text());
        Utils.processTopologyQuads(quads, NG);
        console.log(`${tileUrl} added ${NG.nodes.size - before} nodes to the graph`)
    }
}

async function main() {
    const routesReader = fs.readFileSync("res\\goodroutes.json");
    const routesList = JSON.parse(routesReader);
    /*.pipe(parse({
        from_line: 2,
        delimiter: ";"
    }));*/

    const routes = [];

    for await (let route of routesList) {
        routes.push({from: route.from.id, to: route.to.id});
    }

    const perfWriter = stringify({
        header: true,
        columns: {
            explored: 'explored',
            fetched: 'fetched',
            tiles: 'tiles',
            routeTime: "routeTime",
            setupTime: 'setupTime',
            tileTime: 'tileTime',
            number: 'route#'
        }
    });
    const writestream = fs.createWriteStream("res\\goodResults.csv");
    perfWriter.pipe(writestream);

    let i = 0
    for (let route of routes) {
        
        ("start testing route: " +  i);
        let perf = {}
        perf.queryTime = 0;
        graphStore = GraphStore();
        NG = new NetworkGraph();
        NG.tripleStore = graphStore;
        tileCache = new Set();
        const ops = await fetchOPfromNet([route.from, route.to]);
        await testRoute(ops[0], ops[1], perf);
        i++;
        perf.tiles = tileCache.size;
        perf.nodes = NG.nodes.size;
        perfWriter.write({
            explored: perf.explored,
            fetched: perf.nodes,
            tiles: perf.tiles,
            routeTime: perf.totalTime,
            setupTime: perf.preparation,
            tileTime: perf.queryTime,
            number: i
        })
    }
    console.log(routes);
}

//main();


async function fetchOPfromNet(ops) {
    console.info('INFO: Operational Points geolocation from NetElements');

    const params = ops.map((op, i) => {
        return `
        (<${op}>)`
    }).join('\n');

    const query = `
    PREFIX wgs: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
    PREFIX era: <http://data.europa.eu/949/>
    CONSTRUCT {
        ?OP a era:OperationalPoint.
    } WHERE {
        values (?ne) {${params}
        }
        ?ne ^era:elementPart ?part .
        ?part era:hasImplementation ?OP1.
        optional {
            ?OP1 era:opEnd ?solOP
        }
        bind(coalesce(?solOP, ?OP1) as ?OP)
        ?OP a era:OperationalPoint.
    }`;

    const opts = { headers: { Accept: NTRIPLES } };
    const body = await (await fetch(SPARQL + encodeURIComponent(query), opts)).text();
    const quads = new N3.Parser({ format: 'N-Triples' })
        .parse(await (await fetch(SPARQL + encodeURIComponent(query), opts)).text());
    let results = [];
    for (let quad of quads) {
        results.push(quad.subject.value);
    }
    return results;
}


const RANDOM_QUERY_SET = "res\\icweRoutes\\random-queries_5-17.json.gz"
function loadQuerySet() {
    return new Promise((resolve, reject) => {
        const set = [];
        fs.createReadStream(path.resolve(RANDOM_QUERY_SET))
            .pipe(createGunzip())
            .pipe(jsonlParser.parser())
            .on("data", q => {
                set.push(q.value);
            })
            .on("error", err => reject(err))
            .on("end", () => resolve(set));
    });
}

async function testRoutes() {
    let routes = JSON.parse(fs.readFileSync("res\\testRoutesDFS.json", 'utf8'));
    //let routes = JSON.parse(fs.readFileSync("C:\\Users\\milan\\Documents\\Milan\\Ugent\\1ste Ma\\Masterproef\\test\\CHGeneratorjs\\CHresults\\EraFullNoDup\\testRoutesAstar.json", 'utf8'));
    let goodroutes = [];
    //let routes = await loadQuerySet();

    
    const writestream = fs.createWriteStream("res\\goodroutes.csv");

    //for (let i = Math.floor(routes.length / 2); i >= Math.floor(routes.length / 2) - 100; i--) {
    for (let i = 0; i < routes.length; i++) {
        try {           
            let route = routes[i]; 
            graphStore = GraphStore();
            NG = new NetworkGraph();
            NG.tripleStore = graphStore;
            tileCache = new Set();
            //let ops = await fetchOPfromNet([route.from.id, route.to.id]);
            let ops = await fetchOPfromNet(route);
            if (!ops[0] || !ops[1]) continue;
            let perf = {};
            perf.queryTime = 0;
            let start = performance.now();
            let path = await testRoute(ops[0], ops[1], perf);
            const time = performance.now() - start;
            console.log(perf.tiles);
            console.log(path);
            if (path) {
                writestream.write(`${ops[0]};${ops[1]}}\n`);
                goodroutes.push(route);
                console.log(`route number: ${i} is a good route`);
                console.log("dijkstra rank is: " + perf.explored);
                console.log("total routes found: " + goodroutes.length);
                console.log("time taken to calculate route: ", time);
                console.log("tiles fetched: " + perf.tiles)
            }
        } catch (err) {
            console.log(err);
        }
    }    
    writestream.close();
    fs.writeFileSync('res\\goodroutes.csv', JSON.stringify(goodroutes));
}

async function testR(from, to) {
    let perf = {}
    perf.queryTime = 0;
    let path = await(testRoute(from, to, perf));
    console.log(path);
    console.log(JSON.stringify(perf));
}

const printQueries = async () => {
    let queries = await loadQuerySet();
    console.log(queries);
}

//printQueries();

/*function readNetworkGraph() {
    NG = new NetworkGraph();
    let input = fs.readFileSync("C:\\Users\\milan\\Documents\\Milan\\Ugent\\1ste Ma\\Masterproef\\NG full dump\\OP.trig", 'utf-8');
    let quads = new N3.Parser({ format: 'TriG' }).parse(input);
    Utils.processTopologyQuads(quads, NG);
    quads = new N3.Parser({ format: 'TriG' }).parse(fs.readFileSync("C:\\Users\\milan\\Documents\\Milan\\Ugent\\1ste Ma\\Masterproef\\NG full dump\\SoL.trig", 'utf-8'));
    Utils.processTopologyQuads(quads, NG);
    NG.tripleStore = graphStore;
}*/

//readNetworkGraph();
//testRoutes();


//testRoutes()
//https://linked.ec-dataplatform.eu/describe/?url=http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEFSDA
//https://linked.ec-dataplatform.eu/describe/?url=http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEGKA
//await testR("http://data.europa.eu/949/functionalInfrastructure/operationalPoints/PT38083", "http://data.europa.eu/949/functionalInfrastructure/operationalPoints/LTVilnius")

// test single shortcut
//testR("http://data.europa.eu/949/functionalInfrastructure/operationalPoints/ATEl%20H3", "http://data.europa.eu/949/functionalInfrastructure/operationalPoints/LTVilnius")//", "http://data.europa.eu/949/functionalInfrastructure/operationalPoints/LTVilnius")

//gent ostend
// await testR("http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEFSDA", "http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEGKA");

// deinze kortemark
await testR("http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEFD", "http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEFTK") // BEFD --> BEFTK
// end node of branch causes error1
//testR("http://data.europa.eu/949/functionalInfrastructure/operationalPoints/IT02554", "http://data.europa.eu/949/functionalInfrastructure/operationalPoints/IT02557")
//Mollem --> 
//testR("http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEFKW", "http://data.europa.eu/949/functionalInfrastructure/operationalPoints/BEFEK") // BEFHL --> BEFKW


// get debug map chunk info
/*let quads = new N3.Parser({ format: 'turtle' }).parse(fs.readFileSync('C:\\Users\\milan\\OneDrive - UGent\\1ste ma\\Computergrafiek\\query-result.ttl', 'utf-8'));
Utils.processTopologyQuads(quads, NG);
quads = new N3.Parser({ format: 'turtle' }).parse(fs.readFileSync('C:\\Users\\milan\\OneDrive - UGent\\1ste ma\\Computergrafiek\\SoLcon.ttl', 'utf-8'));
Utils.processTopologyQuads(quads, NG);
quads = new N3.Parser({ format: 'turtle' }).parse(fs.readFileSync('C:\\Users\\milan\\OneDrive - UGent\\1ste ma\\Computergrafiek\\SoLOut.ttl', 'utf-8'));
Utils.processTopologyQuads(quads, NG);
console.log(JSON.stringify(Object.fromEntries(NG.nodes), null, 2));
let output = [];
let idToNumber = new Map();
for (const el of NG.nodes.entries()) {
    let id;
    if (!idToNumber.has(id)) idToNumber.set(el[0], idToNumber.size);
    const obj = { id: idToNumber.get(el[0]), length: el[1].length, rank: el[1].chRank }
    const to = [...el[1].nextNodes].map((next) => {
        if (!idToNumber.has(next.to)) idToNumber.set(next.to, idToNumber.size);
        return idToNumber.get(next.to);
    })
    obj.to = to;
    output.push(obj);
}
console.log(JSON.stringify(output, null, 2));*/

// generate graphviz reperesentation
//let quads = new N3.Parser({ format: 'n3' }).parse(fs.readFileSync('D:\\Nieuwe map\\1371', 'utf-8'));
//Utils.processTopologyQuads(quads, NG);
//NG.visualize("graphvizTestCrossed.dot")

//console.log(translation);

// check for shortcuts in NG
/* Array.from(NG.nodes.entries()).map(([key, value]) => value).filter(value => Array.from(value.nextNodes).some(iri => /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.test(iri.via))) */