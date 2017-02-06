import React from 'react';

class SigmaNode extends React.Component {
  constructor(props){
    super(props);
    this.embedProps = this.embedProps.bind(this);

    // This is what puts nodes on the graph
    props.sigma.graph.addNode(
        {id:props.nid, label:props.nid}
      )

  }

  embedProps(elements: mixed, extraProps) {
        return React.Children.map(elements,
            (element) => React.cloneElement(element, extraProps))
    }

  render() {

    // The sigma instance needs to be passed as props of children
    return (
      <div>
        { this.embedProps(this.props.children, {sigma: this.props.sigma}) }
      </div>)
    }
}

SigmaNode.propTypes = {
  nid: React.PropTypes.string.isRequired,
  hits: React.PropTypes.object.isRequired
}

export default SigmaNode