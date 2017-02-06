import React from 'react';
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaMod from './SigmaMod';

class Graph extends React.Component {

  render() {

   return (
    <div id="graph">
      <Sigma settings={{drawEdges:true}}>
        {
          Object.keys(this.props.nodes).map(
              key=>
                <SigmaMod
                  key={key}
                  details={this.props.nodes[key]}>
                  <RelativeSize initialSize={15}/>
                 <RandomizeNodePositions/>
                </SigmaMod>
              )
        }
      </Sigma>
    </div>
    )
  }
}

Graph.propTypes = {
  graph: React.PropTypes.object.isRequired,
  nodes:React.PropTypes.array.isRequired
}

export default Graph