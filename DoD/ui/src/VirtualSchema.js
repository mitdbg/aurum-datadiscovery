import React, { Component } from 'react';
import VirtualSchemaControl from './VirtualSchemaControl';
import CellValue from './CellValue';


//function TableCell(props) {
//	return (
//		<td>
//			<p>Hi!</p>
//		</td>
//	)
//
//}

function TableRow(props) {
	var row_columns = [];
	for (var j = 0; j < props.columns; j++) {
		//row_columns.push(<TableCell key={j}/>);
		row_columns.push(<CellValue key={j}/>);
	}
	return (
		<tr>{row_columns}</tr>
	)
}

function Table(props) {
	
	var table_rows = [];
	// - 1 because a row is always reserved for the header
	for (var i = 0; i < props.rows - 1; i++) {
		table_rows.push(<TableRow key={i} columns={props.columns}/>);
	}
	
	return (
		<table>
			<thead>
				<TableRow columns={props.columns}/>
			</thead>
			<tbody>
				{table_rows}
			</tbody>
		</table>
	)
}

class VirtualSchema extends React.Component {
	constructor(props) {
		super(props);
		
		this.addRow = this.addRow.bind(this);
		this.removeRow = this.removeRow.bind(this);
		this.addColumn = this.addColumn.bind(this);
		this.removeColumn = this.removeColumn.bind(this);
		
		this.state = {
			rows: 3,
			columns: 3
		};
	}
	
	addRow() {
		this.setState((prevState) => {
				return {rows: prevState.rows + 1};
			}
		);
	}
	
	addColumn() {
		this.setState((prevState) => {
				return {columns: prevState.columns + 1};
			}
		);
	}
	
	removeRow() {
		this.setState((prevState) => {
				if (prevState.rows - 1 < 0) {
					return {rows: 0};
				} else {
					return {rows: prevState.rows - 1};
				}
			}
		);
	}
	
	removeColumn() {
		this.setState((prevState) => {
				if (prevState.columns - 1 < 1) {
					return {columns: 1};
				} else {
					return {columns: prevState.columns - 1};
				}
			}
		);
	}
	
	render() {
		return (
			<div className="VirtualSchemaTable">
				<Table rows={this.state.rows} columns={this.state.columns} />
				<VirtualSchemaControl add_row={this.addRow}
									  remove_row={this.removeRow}
									  add_column={this.addColumn}
									  remove_column={this.removeColumn}
									  />
			</div>
		)
	}
	
}

export default VirtualSchema;