import React from 'react';

class FieldMenu extends React.Component {
  constructor() {
    super();
  }

  render() {
    return(
      <div>
        foo
      </div>
      )
  }
}

export default FieldMenu


FieldMenu.propTypes = {
  nid: React.PropTypes.string,
  sourceName: React.PropTypes.string,
}