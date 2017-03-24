/*jshint esversion: 6 */
import React from 'react';
import FieldSource from './FieldSource';

class Source extends React.Component {


  render() {
    // this code snippet displays the table's score.
    // <span className="source score">{this.props.details.source_res.score.toFixed(2)}</span>
   return (
    <div className="table-block">
      <div className="source title">
        <span className="source name">{this.props.details.source_res.source_name}</span>

      </div>
      {Object
          .keys(this.props.details.field_res)
          .map(key =>
            <FieldSource
              key={key}
              details={this.props.details.field_res[key]}
              addSelection={this.props.addSelection}
            />
          )
        }
    </div>
    )
  }
}


Source.propTypes = {
  details: React.PropTypes.object.isRequired,
  addSelection: React.PropTypes.func.isRequired,
}


export default Source