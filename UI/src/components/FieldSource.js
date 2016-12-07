import React from 'react';

class FieldSource extends React.Component {


  render() {

   return (
    <div className="field title">
      <span className="field name">{this.props.details.field_name}</span>
      <span className="field score">{this.props.details.score.toFixed(2)}</span>
    </div>
    )
  }
}


FieldSource.propTypes = {
  details: React.PropTypes.string.isRequired
}


export default FieldSource