import React, { Component } from 'react';


function ViewButtonPanel(props) {

	const nextView = props.next_view;
	const downloadView = props.download_view;

	return (
		<div>
			<button type="button" className="button-next" data-toggle="tooltip" data-placement="left" data-html="true" title="Show next <em>view</em>" onClick={nextView}>
			    <span className="fas fa-forward"></span>
			</button>
			<button type="button" className="button-download" data-toggle="tooltip" data-placement="left" data-html="true" title="Download <em>view</em>" onClick={downloadView}>
			    <span className="fas fa-file-download" aria-hidden="true"></span>
			</button>
		</div>
	)
}


class ResultViewControl extends Component {
	constructor(props) {
		super(props);
	}

	render() {
		return (
			<div className="ResultViewControl" id='result-view-control'>

		        <ViewButtonPanel next_view={this.props.next_view}
							 download_view={this.props.download_view}
			    />
			</div>
		)
	}

}

export default ResultViewControl;