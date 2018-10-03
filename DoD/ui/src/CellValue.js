import React, { Component } from 'react';

class CellValue extends Component {

    constructor(props) {
        super(props);
        this.rowId = props.rowId;
        this.columnId = props.columnId;
        this.cellKey = this.rowId + "-" + this.columnId;
        this.onVSChange = props.onVSChange;
        this.handleChange = this.handleChange.bind(this);
    }

    findSuggestions(input_text) {
        var payload = {};
        payload['input_text'] = input_text;
        var body = JSON.stringify(payload);

        console.log(payload);

        var response = fetch("http://127.0.0.1:5000/suggest_field", {
            method: "POST",
            mode: "cors",
            cache: "no-cache",
            credentials: 'same-origin',
            headers: {
                "Content-Type": "application/json",
            },
            redirect: "follow",
            referrer: "no-referrer",
            body: body
            })
            .then(res => res.json())
            .then(
                (result) => {
                    console.log("SUCCESS");
                    console.log(result);
                },
                (error) => {
                    console.log("ERROR: " + error);
                }
            )
        console.log(response);
    }

    handleChange(event) {
        // We set the value for this cell
        var newValue = event.target.value;
        this.onVSChange(this.rowId, this.columnId, newValue);
        // Also, if this is the header row, then we suggest options to complete
        if (this.rowId == 0) {
            this.findSuggestions(newValue);
        }
    }


    render() {
        return (
            <td>
			    <input type="text" id="display-name" name="ip-display"
                   pattern="[A-Za-z\s]+"
                   maxLength="30"
                   minLength="0"
                   value={this.value}
                   onChange={this.handleChange}
                />
		    </td>
		)
    }

}

export default CellValue;