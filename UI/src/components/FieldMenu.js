/*jshint esversion: 6 */
import React from 'react';

class FieldMenu extends React.Component {
  constructor() {
    super();
    this.toggleEdgeMenu = this.toggleEdgeMenu.bind(this);
  }

  toggleEdgeMenu(){
    const selectionType = 'field';
    this.props.toggleEdgeMenu(selectionType, this.props.nid);
  }

  render() {
    return(
      <div onClick={this.toggleEdgeMenu} className="field-menu-title">
        {this.props.fieldName} &#9654;
      </div>
      )
  }
}

export default FieldMenu


FieldMenu.propTypes = {
  nid: React.PropTypes.string.isRequired,
  sourceName: React.PropTypes.string.isRequired,
  toggleEdgeMenu: React.PropTypes.func.isRequired,
}