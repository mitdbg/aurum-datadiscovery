import React from 'react';

class SigmaMod extends React.Component {
  constructor(props){
    super(props);

    props.sigma.graph.addNode(
        {id:props.details.nid, label:props.details.label}
      )

    props.sigma.graph.addEdge(
      {id:props.details.nid, source:props.details.nid, target:"n1", label:"bullet"})
  }

  render() {

   return (null)
  }
}

SigmaMod.propTypes = {
  details: React.PropTypes.object.isRequired
}

export default SigmaMod