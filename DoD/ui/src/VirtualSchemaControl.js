import React, { Component } from 'react';


function ButtonPanel(props) {

	const addRow = props.add_row;
	const removeRow = props.remove_row;
	const addColumn = props.add_column;
	const removeColumn = props.remove_column;
	const findView = props.find_view;

	return (
		<div>
		    <div className="row">
		        <div className="col mt-1">
                    <button type="button" className="button-addrow" data-toggle="tooltip" data-placement="left" data-html="true" title="Add <b>ROW</b>" onClick={addRow}>
                        <span className="fas fa-arrow-circle-down"></span>
                    </button>
                    <button type="button" className="button-removerow" data-toggle="tooltip" data-placement="top" data-html="true" title="Remove <b>ROW</b>" onClick={removeRow}>
                        <span className="fas fa-arrow-circle-up"></span>
                    </button>
                </div>
			</div>

			<div className="row">
		        <div className="col mt-1">
                    <button type="button" className="button-addcol" data-toggle="tooltip" data-placement="left" data-html="true" title="Add <b>COLUMN</b>" onClick={addColumn}>
                        <span className="fas fa-arrow-circle-right"></span>
                    </button>
                    <button type="button" className="button-removecol" data-toggle="tooltip" data-placement="right" data-html="true" title="Remove <b>COL</b>" onClick={removeColumn}>
                        <span className="fas fa-arrow-circle-left"></span>
                    </button>
                </div>
            </div>

            <div className="row">
		        <div className="col mt-1">
                    <button type="button" className="button-find" data-toggle="tooltip" data-placement="left" data-html="true" title="Find <em>views</em>" onClick={findView}>
                        <span className="fas fa-play-circle"></span>
                    </button>
                </div>
            </div>
		</div>
	)
}


class VirtualSchemaControl extends Component {
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