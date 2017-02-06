import React from 'react';

class SigmaMod extends React.Component {
  constructor(props){
    super(props);

    this.embedProps = this.embedProps.bind(this);

    props.sigma.graph.addNode(
        {id:props.details.nid, label:props.details.label}
      )

    props.sigma.graph.addEdge(
      {id:props.details.nid, source:props.details.nid, target:"n1", label:"bullet"})
  }

  embedProps(elements: mixed, extraProps) {
        return React.Children.map(elements,
            (element) => React.cloneElement(element, extraProps))
    }

  render() {

   return (
    <div>
      { this.embedProps(this.props.children, {sigma: this.props.sigma}) }
    </div>)
  }
}

SigmaMod.propTypes = {
  details: React.PropTypes.object.isRequired
}

export default SigmaMod