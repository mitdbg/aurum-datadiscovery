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

    handleChange(event) {
        var newValue = event.target.value;
        this.onVSChange(this.rowId, this.columnId, newValue);
    }


    render() {
        return (
            <td>
			    <input type="text" id="display-name" name="ip-display"
                   pattern="[A-Za-z\s]+"
                   maxLength="10"
                   minLength="0"
                   value={this.value}
                   onChange={this.handleChange}
                />
		    </td>
		)
    }

}

export default CellValue;