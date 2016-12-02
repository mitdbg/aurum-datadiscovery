import React from 'react';
import Search from './Search';
import Results from './Results';
import Graph from './Graph';
import Pandas from './Pandas';

class App extends React.Component {
  constructor() {
    super();

    this.updateQuery = this.updateQuery.bind(this);
    // Initial State
    this.state = {
      query: '',
      results: {}
    };
  }

  updateQuery(query) {
    this.setState({ query });
  }


  render() {
    return (
      <div className="aurum">
        <Search
          query={this.state.query}
          updateQuery={this.updateQuery}
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
