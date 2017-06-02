/*jshint esversion: 6 */
import React from 'react';

class FieldSource extends React.Component {


  render() {
    const fieldSource = this.props.details;

   return (
    <div
      className="field title"
      onClick={() => this.props.addSelection(fieldSource)}
    >
      <span className="field name">{this.props.details.field_name}</span>
      <span className="field score">{this.props.details.score.toFixed(2)}</span>
    </div>
    )
  }
}


FieldSource.propTypes = {
  details: React.PropTypes.object.isRequired,
  addSelection: React.PropTypes.func.isRequired,
}


export default FieldSource