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
    // Initial State
    this.state = {
      query: '',
      sources: {},
      edges: [],
    };
  }

  updateQuery(query) {
    this.setState({ query });
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
          />
          <div className="right">
            <Graph />
            <Pandas />
          </div>
        </div>
        <footer>
          footer
        </footer>
      </div>
      )
  }
}

App.propTypes = {
  params: React.PropTypes.object.isRequired
}

export default App;
