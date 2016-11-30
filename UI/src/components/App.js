import React from 'react';
import Search from './Search';
import Results from './Results';
import Graph from './Graph';
import Pandas from './Pandas';

class App extends React.Component {

  render() {
    return (
      <div className="aurum">
        <Search />
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

export default App;
