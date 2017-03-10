import React from 'react';
import SourceMenu from './SourceMenu';
import EdgeMenu from './EdgeMenu';
import TriangleToggle from './TriangleToggle';
import {Sigma, EdgeShapes, ForceAtlas2, RandomizeNodePositions, RelativeSize} from 'react-sigma';
import SigmaNode from './SigmaNode';
import SigmaEdge from './SigmaEdge';
import Pandas from './Pandas';

class Graph extends React.Component {
  constructor() {
    super();
    this.displayNodeDetails = this.displayNodeDetails.bind(this);

    this.state = {
      source: '',
      columns: [],
      clickX: 0,
      clickY: 0,

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
    const node = eventData.data.node;

    // name of the table that was selected
    const source = node.id;
    this.setState({ source });

    // x and y coordinates of the click
    const clickX = node['renderer1:x'];
    var clickY = node['renderer1:y'];
    // console.log(clickX, ', ', clickY)

    this.setState( {clickX })
    this.setState( {clickY })

  }

  render() {

   return (
    <div id="graph">
      <Sigma
        settings={this.state.sigmaSettings}
        renderer="canvas"
        style={{maxWidth:"inherit", height:"100%"}}
        onClickNode={e => this.displayNodeDetails(e)}
        >

        <SourceMenu
          selection={this.props.selection[this.state.source]}
          source={this.state.source}
          x={this.state.clickX}
          y={this.state.clickY}
        />

        <TriangleToggle
          source={this.state.source}
        />

        <EdgeMenu
          source={this.state.source}
          updateQuery={this.props.updateQuery}
          setQueryEdgeType={this.props.setQueryEdgeType}
          updateResult={this.props.updateResult}
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