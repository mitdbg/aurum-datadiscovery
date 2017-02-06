import React from 'react';
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaNode from './SigmaNode';
import SigmaEdge from './SigmaEdge';

class Graph extends React.Component {

  render() {

   return (
    <div id="graph">
      <Sigma settings={{
        // define sigma.js display settings here
        // https://github.com/jacomyal/sigma.js/wiki/Settings
        drawEdges:true,
        minNodeSize:10,
        enableHovering:true}}>
        {
          // cycle through nodes
          Object.keys(this.props.graphNodes).map(
              key=>
                <SigmaNode
                  key={key}
                  node={this.props.graphNodes[key]}>
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
  graphNodes: React.PropTypes.array.isRequired,
  graphEdges: React.PropTypes.array.isRequired
}

export default Graph