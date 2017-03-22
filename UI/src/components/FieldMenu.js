/*jshint esversion: 6 */
import React from 'react';

class FieldMenu extends React.Component {
  constructor() {
    super();
    this.toggleEdgeMenu = this.toggleEdgeMenu.bind(this);
  }

  toggleEdgeMenu(){
    this.props.toggleEdgeMenu(this.props.source, this.props.field, this.props.nid);
  }

  render() {
    return(
      <div onClick={this.toggleEdgeMenu} className="field-menu-title">
        {this.props.field}
        <span className="align-right">&#9654;</span>
      </div>
      )
  }
}

export default FieldMenu


FieldMenu.propTypes = {
  dbName: React.PropTypes.string.isRequired,
  source: React.PropTypes.string.isRequired,
  field: React.PropTypes.string.isRequired,
  nid: React.PropTypes.string.isRequired,
  score: React.PropTypes.number.isRequired,
  toggleEdgeMenu: React.PropTypes.func.isRequired,
}