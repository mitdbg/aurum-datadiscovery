import React from 'react';
import { makeRequest, makeConvert } from '../ajax'
import { renderCanvas } from '../labelCanvas'
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaNode from './SigmaNode';
import SigmaEdge from './SigmaEdge';

class Graph extends React.Component {
  constructor() {
    super();
    this.displayNodeDetails = this.displayNodeDetails.bind(this);
    this.handleRequestResponse = this.handleRequestResponse.bind(this);
    this.handleSourceResponse = this.handleSourceResponse.bind(this);
    this.clearAndDrawNewLabels = this.clearAndDrawNewLabels.bind(this);
    window.makeRequest = makeRequest;
    window.handleRequestResponse = this.handleRequestResponse;

    this.state = {
      source: '',
      columns: [],
      clickX: 0,
      clickY: 0,

      sigmaSettings: {
        minNodeSize:10,
        enableHovering:true,
        defaultNodeColor:'#bababa',
        labelSize:'proportional',
        drawLabels: true,

        // onHover attributes
        borderSize:2,
        defaultNodeBorderColor:'#000000',

        }
    };
  }

  clearAndDrawNewLabels(){
    const selectedColumns = this.props.selection[this.state.source];
    renderCanvas(
      this.state.source, selectedColumns, this.state.columns, this.state.clickX, this.state.clickY);
  }

  handleRequestResponse(response) {
    // little cheat to get the query from the URI
    var url = decodeURI(response.responseURL);
    var query = url.substr(url.lastIndexOf('/')+1);

    const json = JSON.parse(response.responseText);

    this.props.updateQuery(query);
    this.props.updateResult(json);
  }

  // set state for columns after server api response
  handleSourceResponse(response) {
    const json = JSON.parse(response.responseText);
    const columns = json.sources[this.state.source].field_res;

    this.setState( { columns });
    this.clearAndDrawNewLabels();
  }

  // read the graph object. call the api with the id, and then use another
  // callback to process the data.
  displayNodeDetails(eventData){
    // get the position of the click, to draw a box there later
    const node = eventData.data.node;

    // name of the table that was selected
    const source = node.id;
    this.setState({ source });

    // x and y coordinates of the click
    const clickX = node['cam0:x'];
    const clickY = node['cam0:y'];
    this.setState( {clickX })
    this.setState( {clickY })

    makeConvert(this.state.source, this.handleSourceResponse);
  }

  render() {

   return (
    <div id="graph">
      <Sigma
        settings={this.state.sigmaSettings}
        renderer="webgl"
        style={ {maxWidth:"inherit", height:"100%"}}
        onClickNode={e => this.displayNodeDetails(e)}
        >
        {
          // cycle through that are passed as selected items
          Object.keys(this.props.selection).map(
              key=>
                <SigmaNode
                  key={key}
                  nid={key}
                  hits={this.props.selection[key]}
                  >
                  <RelativeSize initialSize={15}/>
                  <RandomizeNodePositions/>
                </SigmaNode>
              )
        }

        {
          // cycle through edges
          Object.keys(this.props.graphEdges).map(
              key=>
                <SigmaEdge
                  key={key}
                  edge={this.props.graphEdges[key]}>
                  <RelativeSize initialSize={15}/>
                  <RandomizeNodePositions/>
                </SigmaEdge>
              )
        }

      </Sigma>
    </div>
    )
  }
}

Graph.propTypes = {
  selection: React.PropTypes.object.isRequired,
  graphEdges: React.PropTypes.array.isRequired,
  updateQuery: React.PropTypes.func.isRequired,
  updateResult: React.PropTypes.func.isRequired,
}

export default Graph