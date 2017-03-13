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

    this.setState({ headings, tableBody });
    console.log('pandas populated');
  }

  clearPandas() {
    this.setState({ headings: [], tableBody: [] });
  }

  componentWillReceiveProps(nextProps){
    if (nextProps.source !== '' && nextProps.source !== this.props.source) {
      makeInspect(nextProps.source, this.populatePandas);
    } else {
      this.clearPandas();
    }
  }


  render() {
    const headings = this.state.headings.map((heading) =>
      <th key={heading}>{heading}</th>
    );

    let rowHTML = (row, rowIndex) => {
      return row.map((cell, index) =>
        <td key={rowIndex.toString() + index.toString()}>{cell}</td>
        )
    }


   return (
    <div id="pandas">
      <table>
        <thead>
          <tr>
            {headings}
          </tr>
        </thead>
        <tbody>
          {this.state.tableBody.map((row, index) => <tr key={'r'+index.toString()}>{rowHTML(row, index)}</tr>)}
        </tbody>
      </table>
    </div>
    )
  }
}

Pandas.propTypes  = {
  source: React.PropTypes.string.isRequired
}

export default Pandas