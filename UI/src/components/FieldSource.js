import React from 'react';

class FieldSource extends React.Component {


  render() {

   return (
    <div className="field_source">
      <span className="field_score">{this.props.details.score}</span>
      <span className="field_name">{this.props.details.field_name}</span>
    </div>
    )
  }
}


FieldSource.propTypes = {
  details: React.PropTypes.string.isRequired
}


export default FieldSource