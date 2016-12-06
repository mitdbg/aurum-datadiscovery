import React from 'react';
import Source from './Source';

class Results extends React.Component {
  constructor() {
    super();
  }


  render() {
    return (
      <div id="left">
        {Object
          .keys(this.props.sources)
          .map(key => <Source details={this.props.sources[key]}/>)}
      </div>
      )
    }
}

Results.propTypes = {
  edges: React.PropTypes.array.isRequired,
  sources: React.PropTypes.object.isRequired,
}

export default Results