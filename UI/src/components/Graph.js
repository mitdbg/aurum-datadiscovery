import React from 'react';
import {Sigma, RandomizeNodePositions, RelativeSize} from 'react-sigmajs';

class Graph extends React.Component {
  render() {

   return (
    <div id="graph">
      <Sigma graph={{nodes:[{id:"n1", label:"Alice"}, {id:"n2", label:"Rabbit"}], edges:[{id:"e1",source:"n1",target:"n2",label:"SEES"}]}} settings={{drawEdges:true}}>
         <RelativeSize initialSize={15}/>
         <RandomizeNodePositions/>
      </Sigma>
    </div>
    )
  }
}

export default Graph