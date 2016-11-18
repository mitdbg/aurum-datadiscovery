import React from 'react'

class App extends React.Component {
  constructor() {
    super();
  }

  render() {
    return (
      <div className="aurum">
        <div className="search-bar">
          search bar
        </div>
        <div className="left-bar">
          Left bar
        </div>
        <div className="graph-view">
          Graph View
        </div>
        <div className="pandas-view">
          Pandas View
        </div>
        <div className="toggle-bar">
        togle bar
        </div>
      </div>
      )
  }
}

export default App;
