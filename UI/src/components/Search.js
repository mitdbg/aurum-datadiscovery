import React from 'react';

class Search extends React.Component {
  render() {

   return (
    <header>
      <input id="search-field" type="text" placeholder="Search by table, column, or keyword" />
    </header>
    )
  }
}

export default Search