import React from 'react';

class Results extends React.Component {
  render() {

   return (
    <div id="left">
    results
    </div>
    )
  }
}

Results.propTypes = {
  result: React.PropTypes.object.isRequired,
}

export default Results