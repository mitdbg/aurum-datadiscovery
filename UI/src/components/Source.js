import React from 'react';
import FieldSource from './FieldSource';

class Source extends React.Component {

  handleClick(e) {

  }

  render() {
   return (
    <div className="table-block">
      <div className="source title">
        <span className="source name">{this.props.details.source_res.source_name}</span>
        <span className="source score">{this.props.details.source_res.score.toFixed(2)}</span>
      </div>
      {Object
          .keys(this.props.details.field_res)
          .map(key => <FieldSource details={this.props.details.field_res[key]}/>)
        }
    </div>
    )
  }
}


Source.propTypes = {
  details: React.PropTypes.string.isRequired,
}


export default Source