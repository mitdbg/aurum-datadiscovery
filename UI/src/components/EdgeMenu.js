/*jshint esversion: 6 */
import React from 'react';
import { makeRequest } from '../ajax';

class EdgeMenu extends React.Component {
  constructor() {
    super();

    this.clickEdgeMenu = this.clickEdgeMenu.bind(this);
    this.handleResponse = this.handleResponse.bind(this);
    this.state = {
      tempQueryEdgeType: '', // temporary query edge. Does not propigate to app unless the query was successful
      tempQuery: '', // temporary query. Does not propigate to app unless the query was successful
    };
  }

  // called if the neighbor search was successful
  // update the query
  // update the results
  // and update the edgeType
  handleResponse(response){
    const json = JSON.parse(response.responseText);

    // update the query diffierently, depending on whether it was a source or field
    if(this.props.field && this.props.nid){
      this.props.updateQuery(this.state.tempQuery, this.props.nid);
      this.props.updateQuery(this.state.tempQuery, this.props.nid);
    } else{
      this.props.updateQuery(this.state.tempQuery, this.props.source);
      this.props.updateQuery(this.state.tempQuery, this.props.source);
    }

    this.props.updateResult(json);
    this.props.setQueryEdgeType(this.state.tempQueryEdgeType);
  }


  clickEdgeMenu(tempQueryEdgeType){
    // update the query diffierently, depending on whether it was a source or field
    let tempQuery = null;
    if(this.props.field && this.props.nid){
      tempQuery = 'neighbor_search("' + this.props.nid + '",' + tempQueryEdgeType + ')';
    } else{
      tempQuery = 'neighbor_search("' + this.props.source + '",' + tempQueryEdgeType + ')';
    }

    makeRequest(tempQuery, this.handleResponse);
    this.setState({tempQueryEdgeType, tempQuery});
  }

  render() {
    let sourceOrFieldNameForMenu = this.props.source;
    if(this.props.field !== ''){
      sourceOrFieldNameForMenu = this.props.field;
    }

    if(this.props.enabled && this.props.source !== ''){
      return(
        <div id="edge-menu">
        <div className="edge-menu-name">
          {sourceOrFieldNameForMenu}
        </div>
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
  enabled: React.PropTypes.bool.isRequired,
  source: React.PropTypes.string,
  field: React.PropTypes.string,
  nid: React.PropTypes.string,
  updateQuery: React.PropTypes.func.isRequired,
  setQueryEdgeType: React.PropTypes.func.isRequired,

}

export default EdgeMenu
