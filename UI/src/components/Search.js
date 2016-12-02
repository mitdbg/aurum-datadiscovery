import React from 'react';
import { makeRequest } from '../ajax'

class Search extends React.Component {
  constructor() {
    super();
    this.handleChange = this.handleChange.bind(this);
    this.updateResults = this.updateResults.bind(this);

    this.state = {
      userQuery: ''
    };
  }



  updateResults(results) {
    this.props.updateQuery(this.state.userQuery);
    const json = JSON.parse(results.responseText);

  }

  handleChange(e){
    const query = e.target.value;
    this.setState({ userQuery: query });
    makeRequest(query, this.updateResults);
  }

  render() {

   return (
    <header>
      <input
        type="text"
        id="search-field"
        placeholder="Search by table, column, or keyword"
        onChange={(e) => this.handleChange(e)}
       />
    </header>
    )
  }
}


Search.propTypes = {
  query: React.PropTypes.string.isRequired,
  updateQuery: React.PropTypes.func.isRequired
}


export default Search