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
      result: {}
    };
  }

  updateQuery(query) {
    this.setState({ query });
  }

  updateResult(result) {
    this.setState( { result });
  }


  render() {
    return (
      <div className="aurum">
        <Search
          query={this.state.query}
          result={this.state.result}
          updateQuery={this.updateQuery}
          updateResult={this.updateResult}
        />
        <div id="middle">
          <Results />
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
