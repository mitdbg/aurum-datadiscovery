import React from 'react';
import { makeConvert } from '../ajax'
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaNode from './SigmaNode';
import SigmaEdge from './SigmaEdge';

class Graph extends React.Component {
  constructor() {
    super();
    this.displayNodeDetails = this.displayNodeDetails.bind(this);
    this.handleSourceResponse = this.handleSourceResponse.bind(this);

    this.state = {
      source: '',
      columns: [],

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

  handleSourceResponse(response) {
    const json = JSON.parse(response.responseText);
    const columns = json.sources[this.state.source].field_res;

    this.setState( {columns });


  }

  // read the graph object. call the api with the id, and then use another
  // callback to process the data.
  displayNodeDetails(eventData){
    // get the position of the click, to draw a box there later
    const node = eventData.data.node;
    const source = node.id;
    this.setState({ source }); // name of the table that was selected

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
  graphEdges: React.PropTypes.array.isRequired
}

export default Graph