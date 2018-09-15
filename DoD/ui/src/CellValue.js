import React, { Component } from 'react';

class CellValue extends React.Component {

    constructor(props) {
        super(props);
        this.state = {value: "hi!"};
    }

    //	<p>{this.state.value}</p>
    // value="hi!" required

    render() {
        return (
            <td>
			    <input type="text" id="display-name" name="ip-display"
                   pattern="[A-Za-z\s]+"
                   maxlength="10"
                   minlength="0"
                   value={this.state.value}
                    />
		    </td>
		)
    }

}

export default CellValue;