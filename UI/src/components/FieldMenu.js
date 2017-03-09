import React from 'react';

class FieldMenu extends React.Component {

  render() {
    return(
      <div className="field-menu-title">
        {this.props.fieldName}
      </div>
      )
  }
}

export default FieldMenu


FieldMenu.propTypes = {
  nid: React.PropTypes.number.isRequired,
  sourceName: React.PropTypes.string.isRequired,
}