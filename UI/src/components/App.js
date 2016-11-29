import React from 'react';
import Search from './Search';
import Results from './Results';
import Graph from './Graph';
import Pandas from './Pandas';

class App extends React.Component {
  constructor() {
    super();
  }

  render() {
    return (
      <div className="aurum">
        <Search />
        <div className="middle-row">
          <Results />
          <div className="right-column">
            <Graph />
            <Pandas />
          </div>
        </div>
      </div>
      )
  }
}

export default App;
