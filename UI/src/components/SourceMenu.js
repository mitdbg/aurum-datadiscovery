/*jshint esversion: 6 */
import React from 'react';
import FieldMenu from './FieldMenu';

class SourceMenu extends React.Component {
  constructor() {
    super();
    this.toggleEdgeMenu = this.toggleEdgeMenu.bind(this);

    this.state = {
      width: 225, // width of the box, in pixels
    };
  }

  toggleEdgeMenu(){
    const selectionType = 'source';
    this.props.toggleEdgeMenu(selectionType, this.props.source);
  }



  render() {
    if(this.props.source !== '') {
      return(
      <div
        id="source-menu"
        style={{width: this.state.width + 'px',
                top: this.props.y + 'px',
                left: this.props.x + this.state.width/2 + 'px'}}>

          <div onClick={this.toggleEdgeMenu} className="source-menu-title">{this.props.source}</div>

          {Object
            .keys(this.props.selection)
            .map(
              key =>
              <FieldMenu
                key={key}
                nid={this.props.selection[key]['nid'].toString()}
                sourceName={this.props.selection[key]['source_name']}
                fieldName={this.props.selection[key]['field_name']}
                dbName={this.props.selection[key]['db_name']}
                score={this.props.selection[key]['score']}
                toggleEdgeMenu={this.props.toggleEdgeMenu}
              />
              )
          }

      </div>
      )
    } else {
      return(
        <div className="display-none"></div>
        )
    }

  }
}

export default SourceMenu

SourceMenu.propTypes = {
  selection: React.PropTypes.object,
  source: React.PropTypes.string,
  x: React.PropTypes.number,
  y: React.PropTypes.number,
  toggleEdgeMenu: React.PropTypes.func.isRequired,
}