import React from 'react';

class SigmaMod extends React.Component {
  constructor(props){
    super(props);
    props.sigma.graph.addNode({id:"n3", label:props.label})
    props.sigma.graph.addEdge({id:"e2", source:"n1", target:"n3", label:"bullet"})
  }

  render() {

   return (null)
  }
}

export default SigmaMod