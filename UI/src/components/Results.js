import React from 'react';

class Results extends React.Component {
  constructor() {
    super();
    this.renderSource = this.renderSource.bind(this);
  }


  renderSource(key){
    return(<div>{key}</div>);
  }


  render() {

  return (
    <div id="left">
      {Object.keys(this.props.sources).map(this.renderSource)}
    </div>
    )
  }
}

Results.propTypes = {
  edges: React.PropTypes.array.isRequired,
  sources: React.PropTypes.object.isRequired,
}

export default Results