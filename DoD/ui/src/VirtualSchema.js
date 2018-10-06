import React, { Component } from 'react';
import VirtualSchemaControl from './VirtualSchemaControl';
import CellValue from './CellValue';
import ResultViews from './ResultViews';
import ResultViewControl from './ResultViewControl';
import ReactDOM from 'react-dom';
import vis from 'vis';


function TableRow(props) {
	var row_columns = [];
	for (var j = 0; j < props.columns; j++) {
		row_columns.push(<CellValue key={j} rowId={props.rowId} columnId={j} onVSChange={props.onVSChange}/>);
	}
	return (
		<tr>{row_columns}</tr>
	)
}

function Table(props) {

	var table_rows = [];
	// - 1 because a row is always reserved for the header
	for (var i = 1; i < props.rows; i++) {
		table_rows.push(<TableRow key={i} rowId={i} columns={props.columns} onVSChange={props.onVSChange}/>);
	}
	
	return (
		<table className="vstable">
			<thead className="vsheader">
				<TableRow key={0} rowId={0} columns={props.columns} onVSChange={props.onVSChange}/>
			</thead>
			<tbody className="vsbody">
				{table_rows}
			</tbody>
		</table>
	)
}

class VirtualSchema extends Component {
	constructor(props) {
		super(props);
		
		this.addRow = this.addRow.bind(this);
		this.removeRow = this.removeRow.bind(this);
		this.addColumn = this.addColumn.bind(this);
		this.removeColumn = this.removeColumn.bind(this);
		this.changeVS = this.changeVS.bind(this);
		this.findView = this.findView.bind(this);
		this.nextView = this.nextView.bind(this);

		this.state = {
			rows: 3,
			columns: 3,
			virtualSchemaValues: {} // key: "i"-"j" -> value
		};
	}

	changeVS(rowId, columnId, newValue) {
	    console.log(this);
	    this.setState((prevState) => {
            console.log(prevState);
            var cellId = rowId + "-" + columnId;
            prevState.virtualSchemaValues[cellId] = newValue;
            return {virtualSchemaValues: prevState.virtualSchemaValues};
	    });
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

    createGraph(listNodes, containerId) {

        // create an array with nodes
        var nodes = new vis.DataSet([
            {id: 1, label: 'Node 1'},
            {id: 2, label: 'Node 2'},
            {id: 3, label: 'Node 3'},
            {id: 4, label: 'Node 4'},
            {id: 5, label: 'Node 5'}
        ]);

        // create an array with edges
        var edges = new vis.DataSet([
            {from: 1, to: 3},
            {from: 1, to: 2},
            {from: 2, to: 4},
            {from: 2, to: 5}
        ]);

        // create a network
        var container = document.getElementById(containerId);

        // provide the data in the vis format
        var data = {
            nodes: nodes,
            edges: edges
        };
        var options = {};

        // initialize your network!
        var network = new vis.Network(container, data, options);
}

	findView() {
	    var vsDefinition = this.state.virtualSchemaValues;
        console.log(vsDefinition);

        document.getElementById('payload').className = 'loader';

        var payload = {};
        payload['payload'] = JSON.stringify(vsDefinition);
        var body = JSON.stringify(payload);

        var response = fetch("http://127.0.0.1:5000/findvs", {
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
                    // Set view
                    var view = result['view'];
                    document.getElementById('payload').className = '';
                    document.getElementById('payload').innerHTML = view;
                    // Set analysis
                    var analysis = result['analysis'];
                    if (analysis != 'no') {
                        var analysis_html = analysis.join(" ");
                        document.getElementById('payload-analysis').className = '';
                        document.getElementById('payload-analysis').innerHTML = analysis_html;

                        // show ResultViewControl
                        document.getElementById('result-view-control').style.visibility = 'visible';

                        // create graph and show it
                        // TODO
                        this.createGraph(null, 'joingraph');
                    }
                },
                (error) => {
                    console.log("ERROR: " + error);
                }
            )
	}


	nextView() {
        var response = fetch("http://127.0.0.1:5000/next_view", {
            method: "POST",
            mode: "cors",
            cache: "no-cache",
            credentials: 'same-origin',
            headers: {
                "Content-Type": "application/json",
            },
            redirect: "follow",
            referrer: "no-referrer",
            body: ""
            })
            .then(res => res.json())
            .then(
                (result) => {
                    var view = result['view'];
                    // if we found a view
                    if (view != 'no-more-views') {
                        document.getElementById('payload').innerHTML = view;
                    }
                    var analysis = result['analysis'];
                    if(analysis != 'no') {
                        var analysis_html = analysis.join(" ");
                        document.getElementById('payload-analysis').className = '';
                        document.getElementById('payload-analysis').innerHTML = analysis_html;
                    }
                    // if no more views are available
                    else {
                        var nomoreviews = "<div class='nomoreview'> <p>No More Views Found! </p> </div>";
                        document.getElementById('payload').innerHTML = nomoreviews;
                        document.getElementById('payload-analysis').innerHTML = '';

                        // hide result view panel
                        document.getElementById('result-view-control').style.visibility = 'hidden';
                    }
                },
                (error) => {
                    console.log("ERROR: " + error);
                }
            )
	}

	downloadView() {
	    // todo
	}

	render() {
		return (
		    <div className="general-wrapper">
		      <div className="row">
		        <div className="col-12 text-center title">

		        </div>
		      </div>

              <div className="row">

                <div className="col-1 left-margin">
                </div>

                <div className="col-6">
                  <div className="VirtualSchemaTable mt-5">
				    <Table rows={this.state.rows} columns={this.state.columns} onVSChange={this.changeVS} />
			      </div>
                </div>

                <div className="col-1 middle-margin">
                </div>

                <div className="col-3">
                  <div className="VirtualSchemaControl mt-5">
                    <VirtualSchemaControl add_row={this.addRow}
									  remove_row={this.removeRow}
									  add_column={this.addColumn}
									  remove_column={this.removeColumn}
									  find_view={this.findView}

				     />
				   </div>
                </div>

                <div className="col-1 right-margin">
                </div>

              </div>

              <div className="row">
                <div className="col-1">
		        </div>
		        <div className="col-6 text-center mt-5">
		            <ResultViews/>
		        </div>
		        <div className="col-4">

                    <div className="row mt-5">
                        <div className="col">
                            <ResultViewControl next_view={this.nextView} download_view={this.downloadView}/>
                        </div>
                    </div>
                    <div className="row mt-2">
                        <div className="col">
                            <div id="payload-analysis">
                            </div>
                            <div id='joingraph'>

                            </div>
                        </div>
                    </div>

		        </div>
		        <div className="col-1">
		        </div>
		      </div>
            </div>
		)
	}
	
}

export default VirtualSchema;