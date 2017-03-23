/*jshint esversion: 6 */
import React from 'react';

class CleanMenu extends React.Component {
  constructor() {
    super();

    this.buttonClick = this.buttonClick.bind(this);

    this.state = {
      width: 225, // width of the box, in pixels
    };
  }

  buttonClick(e){
    console.log(e);
    console.log('CleanMenu button clicked')
  }


  render() {
    if(this.props.enabled === true) {
      return( <div onClick={this.buttonClick}>Clean Menu Visible</div> );
    } else{
      return( <div className="display-none"></div> );
    }
  }
}

CleanMenu.propTypes = {
  enabled: React.PropTypes.bool.isRequired,
}

export default CleanMenu;