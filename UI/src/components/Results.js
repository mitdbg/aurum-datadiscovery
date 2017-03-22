import React from 'react';
import ReactDOM from 'react-dom';
import Source from './Source';
import interact from 'interact.js';

class Results extends React.Component {
  componentDidMount() {
    let handle = document.getElementsByClassName('horizontal-drag-handle')[0]
    interact(ReactDOM.findDOMNode(this))
      .resizable({
        edges: {
          top: false,
          left: false,
          bottom: false,
          right: handle
        },
        onmove: function(event) {
          let desired = event.rect.right
          var decided = Math.min(desired, window.innerWidth)
          // console.log("Desired: " + desired + ", Decided: " + decided + ", Current: " + this.style)
          this.element().style['maxWidth'] = decided + "px"
          let s = window.sigma.instances(0)
          s.refresh()
        }
      });
  }

  render() {
    return (
      <div id="left">
        <div
          className="horizontal-drag-handle"
        />
        {Object
          .keys(this.props.sources)
          .map(
            key =>
            <Source
              key={key}
              details={this.props.sources[key]}
              addSelection={this.props.addSelection}
            />)}
      </div>
      )
    }
}

Results.propTypes = {
  edges: React.PropTypes.array.isRequired,
  sources: React.PropTypes.object.isRequired,
  addSelection: React.PropTypes.func.isRequired,
}

export default Results