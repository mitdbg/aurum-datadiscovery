import React from 'react';

class TriangleToggle extends React.Component {
   constructor() {
    super()
    this.triangleClick = this.triangleClick.bind(this);
  }

  triangleClick (){
    this.props.toggleEdgeMenu()
  }

  render() {

    if(this.props.source !== ''){
      return(
        <div id="triangle" onClick={this.triangleClick}></div>
        )
    } else {
      return(<div className="display-none"></div>)
    }
  }
}


TriangleToggle.propTypes = {
  toggleEdgeMenu: React.PropTypes.func.isRequired,
  source: React.PropTypes.string,
}

export default TriangleToggle
