import React from 'react';
import FieldMenu from './FieldMenu';

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

          <div className="source-title">{this.props.source}</div>
          <hr/>



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