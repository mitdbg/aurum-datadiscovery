import React from 'react';
import Search from './Search';
import Results from './Results';
import Graph from './Graph';
import Pandas from './Pandas';

class App extends React.Component {
  constructor() {
    super();

    this.updateQuery = this.updateQuery.bind(this);
    this.updateResult = this.updateResult.bind(this);
    this.addSelection = this.addSelection.bind(this);
    // this.updateGraphNodes = this.updateGraphNodes.bind(this);
    // Initial State
    this.state = {
      query: '', // the current query
      sources: {}, // the HITs returned from the query
      edges: [], // Aurum edges returned from the query. NOT used for the graph viz.


      // This is what will actually display on the graph
      selection: {}, // the table that the user selected (with HITs inside the obj)
      graphEdges: [], // no way to be populated in the UI yet
      originNode: false, // node that new search results were reached from. null/false/undefined if the search results were reached from the search box

      // graphNodes: [{nid:"n1", label:"Bob"}, {nid:"n2", label:"Markey"}], // used for testing only
      // graphEdges: [{eid: "e1", source:"n1", target:"n2", label:"e1"}],

    };
  }


  // updates the graph state, which  propegates to Graph.js Sigma.props.graph
  // a testing method
  // updateGraphNodes(){
  //   const graphNodes = [
  //       {nid:"n1", label:"Bob"},
  //       {nid:"n2", label:"Markey"},
  //       {nid:"n3", label:"Fizz"},
  //       {nid:"n4", label:"Buzz"},
  //     ];
  //   this.setState({ graphNodes });
  // }


  // This data structure is a bit more complicated.
  // Needs additional setting and getting
  addSelection(selected) {
    // get the tableName/Key
    const tableName = selected['source_name']
    const nid = selected['nid']

    const selection = {...this.state.selection};

    // insert the table if necessary
    if(selection[tableName] === undefined) {
      selection[tableName] = {};
    }

    // insert the field
    selection[tableName][nid] = selected;

    this.setState({ selection });
  }

  removeSelection(nid) {
    const selection = {...this.state.selection};
    delete selection[nid];
    this.setState({ selection }) ;
  }

  clearSelection(){
    this.setState({selection : {}})
  }


  updateQuery(query, originNode) {
    this.setState({ query });
    this.setState({originNode})
    this.context.router.transitionTo(`/${query}`);
  }

  updateResult(result) {
    var sources = result['sources'];
    var edges = result['edges'];
    this.setState( { sources });
    this.setState( { edges });
  }


  render() {
    return (
      <div className="aurum">
        <Search
          query={this.state.query}
          sources={this.state.sources}
          edges={this.state.edges}
          updateQuery={this.updateQuery}
          updateResult={this.updateResult}
        />
        <div id="middle">
          <Results
            sources={this.state.sources}
            edges={this.state.edges}
            selectHit={this.state.selected}
            addSelection={this.addSelection}
          />
          <div id="right">
            <Graph
              graphEdges={this.state.graphEdges}
              selection={this.state.selection}
              updateQuery={this.updateQuery}
              updateResult={this.updateResult}
            />
            <Pandas />
          </div>
        </div>
        <footer>
        </footer>
      </div>
      )
  }
}

App.contextTypes = {
  router: React.PropTypes.object
}

export default App;
