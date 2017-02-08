import React from 'react';
import Source from './Source';

class Results extends React.Component {

  render() {
    return (
      <div id="left">
      <div id="result-description">Keyword "school"</div>
        {Object
          .keys(this.props.sources)
          .map(
            key =>
            <Source
              key={key}
              details={this.props.sources[key]}
              addSelection={this.props.addSelection}
            />)}
      </div>
      )
    }
}

Results.propTypes = {
  edges: React.PropTypes.array.isRequired,
  sources: React.PropTypes.object.isRequired,
  addSelection: React.PropTypes.func.isRequired,
}

export default Results