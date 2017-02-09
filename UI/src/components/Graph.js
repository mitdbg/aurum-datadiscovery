import React from 'react';
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaNode from './SigmaNode';
import SigmaEdge from './SigmaEdge';

class Graph extends React.Component {
  constructor() {
    super();

    this.state = {
      sigmaSettings: {
      minNodeSize:10,
      enableHovering:true,
      defaultNodeColor:'#bababa',
      labelSize:'proportional',
      // labelAlignment:'bottom',

      // onHover attributes
      borderSize:2,
      defaultNodeBorderColor:'#000000',

      }
    };
  }

  render() {

   return (
    <div id="graph">
      <Sigma
        settings={this.state.sigmaSettings}
        renderer="webgl"
        style={ {maxWidth:"inherit", height:"100%"} }
        >
        {
          // cycle through that are passed as selected items
          Object.keys(this.props.selection).map(
              key=>
                <SigmaNode
                  key={key}
                  nid={key}
                  hits={this.props.selection[key]}>
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