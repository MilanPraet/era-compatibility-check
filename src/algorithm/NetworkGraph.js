/*export type shortcut {
    id: String,
    nextNode: node
}*/

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
                mesoElement: null,
                length: null,
                lngLat: null,
                lineNationalId: null,
                chRank: null
            };
        }
        
        if (node.lngLat) n.lngLat = node.lngLat;
        if (node.length) n.length = parseFloat(node.length);
        if (node.mesoElement) n.mesoElement = node.mesoElement;
        if (node.nextNode) n.nextNodes.add(node.nextNode); //{via, to}
        if (node.lineNationalId) n.lineNationalId = node.lineNationalId;        
        if (node.chRank) n.chRank = node.chRank;

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