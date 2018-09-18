import React, { Component } from 'react';


function ButtonPanel(props) {

	const addRow = props.add_row;
	const removeRow = props.remove_row;
	const addColumn = props.add_column;
	const removeColumn = props.remove_column;
	const findView = props.find_view;

	return (
		<div>
			<button type="button" onClick={addRow}>Add Row</button>
			<button type="button" onClick={removeRow}>Remove Row</button>
			<button type="button" onClick={addColumn}>Add Column</button>
			<button type="button" onClick={removeColumn}>Remove Column</button>
			<button type="button" onClick={findView}>Find View</button>
		</div>
	)
}


class VirtualSchemaControl extends React.Component {
	constructor(props) {
		super(props);
	}
	
	render() {
		return (
			<div className="VirtualSchemaControl">
				<ButtonPanel add_row={this.props.add_row}
							 remove_row={this.props.remove_row}
							 add_column={this.props.add_column}
							 remove_column={this.props.remove_column}
							 find_view={this.props.find_view}
							 />
			</div>
		)
	}
	
}

export default VirtualSchemaControl;