import React from 'react';

// This class exists because the Sigma Component can't deal with
// being passed false or null. Instead, it has to be passed
// an empty div, if nothing should be rendered. 0_o
class EmptyDiv extends React.Component {


  render() {
   return (
    <div display="none">
    </div>
    )
  }
}




export default EmptyDiv