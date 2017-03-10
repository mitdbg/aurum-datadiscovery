import React from 'react';
import { makeInspect } from '../ajax'

class Pandas extends React.Component {
  constructor() {
    super();
    this.populatePandas = this.populatePandas.bind(this); // fill the pandas pane with the returned table data
    this.clearPandas = this.clearPandas.bind(this); // clear the pandas pane

    this.state = {
      headings: [],
      tableBody: [],

    }
  }


  populatePandas(response){
    const data = JSON.parse(response.response);
    const headings = Object.keys(data);
    var numRows = Object.keys(data[headings[0]]).length; // assume all rows are the same length

    var tableBody = []
    debugger;
    for (var j = 0; j < numRows; j++) {
        var row = [];
        var strindex = j.toString();

        for (var i = 0; i < headings.length; i++) {
          var heading = headings[i];
          var cell = data[heading][strindex];
          row.push(cell);
        }
        tableBody.push(row);
    }

    this.setState( {headings} );
    this.setState({ tableBody });
  }

  clearPandas() {
    console.log('clearPandas called');
  }

  componentDidUpdate(){
    if (this.props.source !== '') {
      console.log('makeInspect called');
      makeInspect(this.props.source, this.populatePandas);
    } else {
      this.clearPandas();
    }

  }

  render() {

   return (
    <div id="pandas">
    <table>
      <thead>
          {this.state.headings.forEach((heading) => <tr>heading</tr>)}
      </thead>
    </table>
    </div>
    )
  }
}

Pandas.propTypes  = {
  source: React.PropTypes.string.isRequired
}

export default Pandas