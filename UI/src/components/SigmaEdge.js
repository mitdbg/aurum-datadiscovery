import React from 'react';

class SigmaEdge extends React.Component {
  constructor(props){
    super(props);
    this.embedProps = this.embedProps.bind(this);

    // This puts edges on the graph
    props.sigma.graph.addEdge({
      id:props.edge.eid,
      source:props.edge.source,
      target:props.edge.target,
      label:props.edge.label,
      color: props.edge.color,
      size: 4,
    })
  }

  embedProps(elements: mixed, extraProps) {
        return React.Children.map(elements,
            (element) => React.cloneElement(element, extraProps))
    }

  render() {

    // The sigma instance needs to be passed as props of children
    // Can see how it's done in the react-sigma project here:
    // https://github.com/dunnock/react-sigma/blob/master/src/LoadJSON.js#L61
    return (
      <div>
        { this.embedProps(this.props.children, {sigma: this.props.sigma}) }
      </div>)
  }
}

SigmaEdge.propTypes = {
  edge: React.PropTypes.object.isRequired,
}

export default SigmaEdge