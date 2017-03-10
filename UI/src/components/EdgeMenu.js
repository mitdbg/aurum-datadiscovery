import React from 'react';
import { makeRequest } from '../ajax';

class EdgeMenu extends React.Component {
  constructor() {
    super();

    this.clickEdgeMenu = this.clickEdgeMenu.bind(this);
    this.handleResponse = this.handleResponse.bind(this);
    this.state = {
      tempQueryEdgeType: '', // temporary query edge. Does not propigate to app unless the query was successful
    }
  }

  // called if the neighbor search was successful
  // update the query
  // update the results
  // and update the edgeType
  handleResponse(response){
    const json = JSON.parse(response.responseText);
    this.props.updateQuery(this.state.userQuery, false);
    this.props.updateResult(json);
    this.props.setQueryEdgeType(this.state.tempQueryEdgeType);
  }


  clickEdgeMenu(tempQueryEdgeType){
    var query = 'neighbor_search("' + this.props.source + '",' + tempQueryEdgeType + ')';
    makeRequest(query, this.handleResponse);
    this.setState({tempQueryEdgeType});

  }

  render() {

    if(this.props.source !== ''){
      return(
        <div id="edge-menu">
          <div
            className="edge-menu-title"
            onClick={() => this.clickEdgeMenu('schema_sim')}>
            Similar Context
          </div>
          <div
            className="edge-menu-title"
            onClick={() => this.clickEdgeMenu('content_sim')}>
            Similar Content
          </div>
          <div
            className="edge-menu-title"
            onClick={() => this.clickEdgeMenu('pkfk')}>
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
