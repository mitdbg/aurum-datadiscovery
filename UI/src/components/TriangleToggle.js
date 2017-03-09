import React from 'react';

class TriangleToggle extends React.Component {

  render() {

    if(this.props.source !== ''){
      return(
        <div id="triangle"></div>
        )
    } else {
      return(<div className="display-none"></div>)
    }
  }
}


TriangleToggle.propTypes = {
  source: React.PropTypes.string,
}

export default TriangleToggle
