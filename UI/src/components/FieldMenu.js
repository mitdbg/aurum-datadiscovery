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
        <span className="align-right">&#9654;</span>
        <div style={{width: this.props.parentWidth - 10 + 'px'}}>
          {this.props.field}
        </div>

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
  parentWidth: React.PropTypes.number.isRequired
}