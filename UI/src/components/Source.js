import React from 'react';
import FieldSource from './FieldSource';

class Source extends React.Component {

  render() {
   return (
    <div>
      <div className="source_title">
        <span className="source_score">{this.props.details.source_res.score}</span>
        <span className="source_name">{this.props.details.source_res.source_name}</span>
        {Object
          .keys(this.props.details.field_res)
          .map(key => <FieldSource details={this.props.details.field_res[key]}/>)
        }
      </div>
    </div>
    )
  }
}


Source.propTypes = {
  details: React.PropTypes.string.isRequired,
}


export default Source