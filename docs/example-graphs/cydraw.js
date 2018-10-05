
function cydraw(elements) {

    var cy = cytoscape({
        container: document.getElementById('cy'),
        elements: elements,
        minZoom: 0.8, maxZoom: 1.4,
        style: [
            {
                selector: 'node',
                style: {
                    'background-color': '#666',
                    'label': 'data(id)'
                }
            },
            {
                selector: 'node[viz_color]',
                style: { 'background-color': 'data(viz_color)' }
            },
            {
                selector: 'edge',
                style: {
                    width: 1.4,
                    lineColor: "lightblue",
                    'target-arrow-color': '#ccc',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier'
                }
            }
        ],
        layout: {
            name: 'cose',
            numIter: 100,
            directed: true,
            nodeDimensionsIncludeLabels: true
        }
    });

    if (typeof(cy.navigator) !== 'undefined')
        cy.navigator({});

}  
