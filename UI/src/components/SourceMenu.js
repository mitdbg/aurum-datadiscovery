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
    this.props.toggleEdgeMenu(this.props.source, '', '');
  }



  render() {
    if(this.props.source !== '') {
      return(
      <div
        id="source-menu"
        style={{width: this.state.width + 'px',
                top: this.props.y + 'px',
                left: this.props.x - this.state.width/2 + 'px'}}>

          <div
            onClick={this.toggleEdgeMenu}
            className="source-menu-title">{this.props.source}
            <span className="align-right">&#9654;</span>
          </div>

          {Object
            .keys(this.props.selection)
            .map(
              key =>
              <FieldMenu
                key={key}
                dbName={this.props.selection[key]['db_name']}
                source={this.props.selection[key]['source_name']}
                field={this.props.selection[key]['field_name']}
                nid={this.props.selection[key]['nid'].toString()}
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
  field: React.PropTypes.string,
  nid: React.PropTypes.string,
  x: React.PropTypes.number,
  y: React.PropTypes.number,
  toggleEdgeMenu: React.PropTypes.func.isRequired,
}