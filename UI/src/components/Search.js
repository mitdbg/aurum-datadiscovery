import React from 'react';

class Search extends React.Component {
  render() {

   return (
    <div className="search-row">
      <input type="text" placeholder="Search by table, column, or keyword" />
    </div>
    )
  }
}

export default Search