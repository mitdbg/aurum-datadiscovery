/*jshint esversion: 6 */

import React from 'react';
import SourceMenu from './SourceMenu';
import EdgeMenu from './EdgeMenu';
import {Sigma, EdgeShapes, ForceAtlas2, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaNode from './SigmaNode';
import SigmaEdge from './SigmaEdge';
import Pandas from './Pandas';

class Graph extends React.Component {
  constructor() {
    super();
    this.displayNodeDetails = this.displayNodeDetails.bind(this);
    this.toggleEdgeMenu = this.toggleEdgeMenu.bind(this);

    this.state = {
      source: '', // name of the table selected
      field: '', // field in the table selected.
      nid: '', // nid of the field BUT NOT SOURCE selected, if a field is selected
      columns: [],
      clickX: 0,
      clickY: 0,
      edgeMenuEnabled: false,

      sigmaSettings: {
        // normal node attrs
        minNodeSize:10,
        enableHovering:true,
        defaultNodeColor:'#bababa',
        labelSize:'proportional',
        drawLabels: true,


        // onHover node attrs
        borderSize:2,
        defaultNodeBorderColor:'#000000',

        // normal edge attrs
        // minEdgeSize: 4, // no effect
        // defaultEdgeColor: 'orange', // no effect
        drawEdgeLabels: true, // works
        drawEdges: true, // works

        // onHover node attrs
        enableEdgeHovering: true,
        edgeHoverSizeRatio: 3,
        // edgeHoverColor: 'black',
        // edgeHoverExtremities: false,
        }
    };
  }

  // read the graph object. call the api with the id, and then use another
  // callback to process the data.
  displayNodeDetails(eventData){
    // get the position of the click, to draw a box there later
    const { data: { node } } = eventData;

    // name of the table that was selected
    const source = node.id;

    // x and y coordinates of the click
    const clickX = node['renderer1:x'];
    var clickY = node['renderer1:y'];
    // console.log(clickX, ', ', clickY)

    this.setState({ clickX, clickY, source });
  }

  toggleEdgeMenu(source, field, nid){

    // the user clicked the same Source/Field Menu item again. Dismiss the EdgeMenu
    if (this.state.edgeMenuEnabled === true && this.state.source === source && this.state.field === field && this.state.nid === nid){
      this.setState({ edgeMenuEnabled: false, field: '', nid: '' });
    }
    // the user clicked a new menu item. UpdateVariables and show the menu
    else{
      this.setState({ field, nid, edgeMenuEnabled: true });
    }

  }

  render() {

  // SourceMenu and EdgeMenu ultimately need to exist outside of the <Sigma>
  // component, so that they can be rendered above it
  return (
    <div id="graph">
      <Sigma
        settings={this.state.sigmaSettings}
        renderer="canvas"
        style={{maxWidth:"inherit", height:"100%"}}
        onClickNode={e => this.displayNodeDetails(e)}
        >


        <SourceMenu
          // the source, and field names the user selected from the results panel
          selection={this.props.selection[this.state.source]}
          source={this.state.source} // the node the user selected in the graph
          field={this.state.field} // the field (if any) the user selected in the menu in the graph
          nid={this.state.nid} // the nid of the above field
          x={this.state.clickX}
          y={this.state.clickY}
          toggleEdgeMenu={this.toggleEdgeMenu}
        />

        <EdgeMenu
          source={this.state.source}
          field={this.state.field}
          nid={this.state.nid}
          updateQuery={this.props.updateQuery}
          setQueryEdgeType={this.props.setQueryEdgeType}
          updateResult={this.props.updateResult}
          enabled={this.state.edgeMenuEnabled}
        />

        {
          // cycle through that are passed as selected items
          Object.keys(this.props.selection).map(
              key=>
                <SigmaNode
                  key={key}
                  nid={key}
                  hits={this.props.selection[key]}
                  >
                  <RelativeSize initialSize={10}/>
                  <RandomizeNodePositions>
                    <ForceAtlas2 iterationsPerRender={1} timeout={600}/>
                  </RandomizeNodePositions>
                </SigmaNode>
              )
        }

        {
          // cycle through edges
          Object.keys(this.props.graphEdges).map(
              key=>
                <SigmaEdge
                  key={key}
                  edge={this.props.graphEdges[key]}>
                  <EdgeShapes default="curvedArrow"/>
                </SigmaEdge>
              )
        }

      </Sigma>
      <Pandas
        source={this.state.source}
      />


    </div>
    )
  }
}

Graph.propTypes = {
  selection: React.PropTypes.object.isRequired,
  graphEdges: React.PropTypes.array.isRequired,
  updateQuery: React.PropTypes.func.isRequired,
  updateResult: React.PropTypes.func.isRequired,
  setQueryEdgeType: React.PropTypes.func.isRequired,
}

export default Graph