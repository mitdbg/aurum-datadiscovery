/*jshint esversion: 6 */
import React from 'react';
import { makeClean } from '../ajax';

class CleanMenu extends React.Component {
  constructor() {
    super();

    this.buttonClick = this.buttonClick.bind(this);

    this.state = {
      width: 225, // width of the box, in pixels
    };
  }

  buttonClick(e){
    // console.log(e);
    makeClean(this.props.source, this.props.target, (r)=>console.log(r));
    debugger;
  }


  render() {
    if(this.props.enabled === true) {
      return( <div id="clean-menu" onClick={this.buttonClick}>Clean Menu Visible</div> );
    } else{
      return( <div className="display-none"></div> );
    }
  }
}

CleanMenu.propTypes = {
  enabled: React.PropTypes.bool.isRequired, // should the menu display?
  x: React.PropTypes.number, // coordinates of the click event
  y: React.PropTypes.number, // coordinates of the click event
  source: React.PropTypes.string, // the edge clicked goes from here...
  target: React.PropTypes.string // ...to here
}

export default CleanMenu;