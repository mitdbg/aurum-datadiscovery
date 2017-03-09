import React from 'react';
import FieldMenu from './FieldMenu';
import EdgeMenu from './EdgeMenu';

class SourceMenu extends React.Component {
  constructor() {
    super();

    this.state = {
      width: 225, // width of the box, in pixels
    }
  }


  render() {
    return(
      <div
        className="box"
        id="relbox"
        style={{width: this.state.width + 'px',
                top: this.props.y + 'px',
                left: this.props.x + this.state.width/2 + 'px'}}>

          <div className="source-menu-title">{this.props.source}</div>

          {Object
            .keys(this.props.selection)
            .map(
              key =>
              <FieldMenu
                key={key}
                nid={this.props.selection[key]['nid']}
                sourceName={this.props.selection[key]['source_name']}
                fieldName={this.props.selection[key]['field_name']}
                dbName={this.props.selection[key]['db_name']}
                score={this.props.selection[key]['score']}
              />
              )
          }

          <div id="triangle"></div>
          <EdgeMenu />

      </div>
      )
  }
}

export default SourceMenu


SourceMenu.propTypes = {
  selection: React.PropTypes.object,
  source: React.PropTypes.string,
  x: React.PropTypes.number,
  y: React.PropTypes.number
}