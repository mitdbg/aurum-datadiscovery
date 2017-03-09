import React from 'react';

class EdgeMenu extends React.Component {

  render() {

    if(this.props.source !== ''){
      return(
        <div id="edge-box">
          <div className="edge-menu-title">
            Similar Context
          </div>
          <div className="edge-menu-title">
            Similar Content
          </div>
          <div className="edge-menu-title">
            PKFK
          </div>
        </div>
        )
    } else {
      return(<div className="display-none"></div>)
    }
  }
}


EdgeMenu.propTypes = {
  source: React.PropTypes.string,
  updateQuery: React.PropTypes.func.isRequired,
  setQueryEdgeType: React.PropTypes.func.isRequired,
}

export default EdgeMenu
