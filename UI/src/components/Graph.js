import React from 'react';
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaMod from './SigmaMod';

class Graph extends React.Component {
  render() {

   return (
    <div id="graph">
      <Sigma graph={this.props.graph} settings={{drawEdges:true}}>
          <SigmaMod label="Shoots"/>
          <RelativeSize initialSize={15}/>
          <RandomizeNodePositions/>
      </Sigma>
    </div>
    )
  }
}

Graph.propTypes = {
  graph: React.PropTypes.object.isRequired
}

export default Graph