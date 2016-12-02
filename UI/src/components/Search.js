import React from 'react';
import { makeRequest } from '../ajax'

class Search extends React.Component {
  constructor() {
    super();
    this.handleChange = this.handleChange.bind(this);
    this.handleResponse = this.handleResponse.bind(this);

    this.state = {
      // userQuery is what the user typed in. This query might be invalid.
      userQuery: ''

      // results are handled at the App level.
    };
  }



  handleResponse(response) {
    const json = JSON.parse(response.responseText);

    this.props.updateQuery(this.state.userQuery);
    this.props.updateResult(json);

  }

  handleChange(e){
    debugger;
    const query = e.target.value;
    this.setState({ userQuery: query });
    makeRequest(query, this.handleResponse);
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
  result: React.PropTypes.object.isRequired,
  updateQuery: React.PropTypes.func.isRequired,
  updateResult: React.PropTypes.func.isRequired
}


export default Search