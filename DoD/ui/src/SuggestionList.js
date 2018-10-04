import React, { Component } from 'react';


class SuggestionList extends Component {

    constructor(props) {
        super(props);
        this.suggestions = props.suggestions;
        this.rowId = props.rowId;
        this.colId = props.colId;
        this.onSuggestionClick = props.onSuggestionClick;
        this.handleClick = this.handleClick.bind(this);
    }

    handleClick(event) {
        // We set the value for this cell
        var test = console.log(event);
        var newValue = event.target.innerHTML;
//        console.log("user clicked in the list: " + newValue);
//        console.log(event.target);
//        console.log(event.target.innerHTML);
        this.onSuggestionClick(this.rowId, this.colId, newValue);
    }

//    <p>" from " + entry['originRelation']}</p>

    render() {
        var listEntries = [];
        for(var i = 0; i < this.suggestions.length; i++) {
            var entry = this.suggestions[i];
            listEntries.push(<li key={entry['id']} onClick={this.handleClick}>{entry['attributeName']}</li>);
        }
        return (
            <ul>
                {listEntries}
		    </ul>
		)
    }
}

export default SuggestionList;