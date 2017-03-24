/*jshint esversion: 6 */
import React from 'react';

class EdgeToolTip extends React.Component {
  constructor() {
    super();

    this.state = {
      width: 140, // width of the box, in pixels
    };
  }



  render() {
    // 2 seems to work, while the derived number seems to be wrong
    // ... so we're using two for a scaling factor.
    // const canvas = document.getElementById('graph').getElementsByTagName('canvas')[0];
    // const xScalingFactor = canvas.width/canvas.clientWidth;
    // const yScalingFactor = canvas.height/canvas.clientHeight;
    // console.log(xScalingFactor + ', ' + yScalingFactor);


    const labelText = this.props.label;
    let fromText;
    let toText;
    if (this.props.formField && this.props.formField !== ''){
      fromText = 'from: ' + this.props.fromField;
    } else{
      fromText = 'from: ' + this.props.fromSource;
    }

    if (this.props.formField && this.props.formField !== ''){
      toText = 'to: ' + this.props.toField;
    } else{
      toText = 'to: ' + this.props.toSource;
    }


    if(this.props.enabled === true) {
      return(
        <div
          id="clean-menu"
          onClick={this.buttonClick}
          style={
            {top: this.props.y/2 + 'px',
            left: this.props.x/2 + this.state.width/2 + 'px',
            width: this.state.width + 'px',
            }}
          >
          <div>{labelText}</div>
          <div>{fromText}</div>
          <div>{toText}</div>

          </div>
      );
    } else{
      return( <div className="display-none"></div> );
    }
  }
}

EdgeToolTip.propTypes = {
  x: React.PropTypes.number, // coordinates of the hover event
  y: React.PropTypes.number, // coordinates of the hover event
  label: React.PropTypes.string, // type of edge
  fromSource: React.PropTypes.string, // the edge hover goes from here...
  fromField: React.PropTypes.string,
  toSource: React.PropTypes.string, // ...to here
  toField: React.PropTypes.string,
  score: React.PropTypes.number, // the relevance score the edge has
  enabled: React.PropTypes.bool.isRequired, // should the menu display?
}

export default EdgeToolTip;