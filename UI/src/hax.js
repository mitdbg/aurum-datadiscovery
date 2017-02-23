// hacky little file that does things like clear the labels cxt

var clickables = [];

class Clickable {
  constructor(x1, y1, x2, y2, onClick){
    this.x1 = x1;
    this.x2 = x2;
    this.y1 = y1;
    this.y2 = y2;
    this.onClick = onClick;
  }
}

export function clearLabels(){
  const labelCanvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  // get the full height and width of the window
  const width = document.documentElement.clientWidth;
  const height = document.documentElement.clientHeight;

  labelCanvas.clearRect(0, 0, width, height)

}

export function drawMainMenu(sourceName, selectedColumns, allColumns, x, y){

  // get the sigma-mouse canvas
  const mouseCanvas = document.getElementsByClassName('sigma-mouse')[0]

  // clone it and modify
  var newCanvas = mouseCanvas.cloneNode(true);
  newCanvas.className = 'aurum-overlay';
  newCanvas.id = 'aurum-overlay';
  const cxt = newCanvas.getContext('2d');

  // assume everything is retna for now, and scale accordingly
  // In the future, it may be possible to get the transformation of the
  // mouseCanvas 2d context, using cxt.currentTransform.
  // Unofrtunately, this is not supported in most browsers yet.
  // For now, we use the less sophisticated window.devicePixelRatio
  const scale = window.devicePixelRatio;
  cxt.scale(scale, scale);

  // put the clone on top of the mouse canvas
  insertAfter(mouseCanvas, newCanvas);

  // define elements to be drawn
  var box = {}

  box.margin = {}
  box.margin.top = 15;

  box.padding = {}
  box.padding.left = 3;
  box.padding.right = 3;
  box.padding.top = 3;
  box.padding.bottom = 5;

  box.width = 225;
  box.height = null;

  box.x = x - box.width/2;
  box.y = y + box.margin.top;


  // table name
  var source = {}
  source.lineHeight = 14;
  source.fillStyle = 'black';
  source.textAlign = 'center';
  source.lineSpace = 0;
  source.marginBottom = 9;
  source.name = sourceName;

  // set cxts variables before getLines
  cxt.fillStyle = source.fillStyle;
  cxt.font = source.lineHeight + 'px sans-serif';
  cxt.textBaseline = 'top';
  cxt.textAlign = source.textAlign;

  // getLines
  source.lines = getLines(cxt, source.name, box.width - box.padding.left - box.padding.right)
  source.y = box.padding.top + box.y;

  // line
  var line = {}
  line.marginBottom = 2;
  line.y = source.lines.length * (source.lineHeight + source.lineSpace) + source.marginBottom + source.y

  // columns selected
  var field = {}
  field.lineHeight = 12;
  field.fillStyle = 'black';
  field.textAlign = 'left';
  field.lineSpace = 2;
  field.selected = selectedColumns;
  field.y = line.y + line.marginBottom;

  // columns not selected
  var fieldUnselected = {}
  fieldUnselected.lineHeight = 12;
  fieldUnselected.fillStyle = 'black'
  fieldUnselected.textAlign = 'left';
  fieldUnselected.lineSpace = 2;
  fieldUnselected.num = Object.keys(allColumns).length - Object.keys(field.selected).length
  fieldUnselected.y = Object.keys(field.selected).length * (field.lineHeight + field.lineSpace) + field.y

  box.height = fieldUnselected.y - box.y + fieldUnselected.lineHeight + fieldUnselected.lineSpace;

  // triangle menu
  var triangle = {};
  triangle.fillStyle = '#b2b2b2';
  triangle.width = 17;
  triangle.height = 10;
  triangle.marginLeft = 5;
  triangle.marginTop = (line.y - source.y)/2 - triangle.height;

  triangle.left = {};
  triangle.left.x = box.x + box.width + triangle.marginLeft;
  triangle.left.y = box.y + triangle.marginTop;

  triangle.right = {};
  triangle.right.x = triangle.left.x + triangle.width;
  triangle.right.y = triangle.left.y;

  triangle.bottom = {}
  triangle.bottom.x = triangle.left.x + triangle.width/2;
  triangle.bottom.y = triangle.left.y + triangle.height;

  cxt.fillStyle = '#f2f2f2';
  drawRectangleBackground(cxt, box);

  cxt.strokeStyle = 'black';
  drawRectangleBorder(cxt, box);

  cxt.fillStyle = triangle.fillStyle;
  drawTriangle(cxt, box, triangle);

  cxt.fillStyle = source.fillStyle;
  cxt.font = source.lineHeight + 'px sans-serif';
  cxt.textBaseline = 'top';
  cxt.textAlign = source.textAlign;
  drawSource(cxt, box, source);

  drawLine(cxt, box, line);

  cxt.fillStyle = field.fillStyle;
  cxt.font = field.lineHeight + 'px sans-serif';
  cxt.textBaseline = 'top';
  cxt.textAlign = field.textAlign;
  drawNamedFields(cxt, box, field, false);

  cxt.fillStyle = 'gray';
  drawNumUnselectedFields(cxt, box, fieldUnselected);

  // add an event handler
  newCanvas = document.getElementById('aurum-overlay')
  newCanvas.addEventListener('click', (event)=>handleClick(event, box, triangle, newCanvas));
  newCanvas.addEventListener('mousemove', (event)=>handleMousemove(event, box, triangle));
}

// draw the table name
function drawSource(cxt, box, source) {
  // iterate through lines
  var offset = 0;
  for (var i = 0; i < Object.keys(source.lines).length; i++) {
    const line = source.lines[i]
    cxt.fillText(line, box.x + box.width/2, source.y + offset);
    offset += source.lineHeight + source.lineSpace;
  }
}

// draw a horizontal line
function drawLine(cxt, box, line) {
  cxt.beginPath();
  cxt.moveTo(box.x, line.y);
  cxt.lineTo(box.x + box.width, line.y);
  cxt.stroke();
}

// draw the columns that were selected
function drawNamedFields(cxt, box, field, seperators){
  var offset = 0;
  for (var k in field.selected){

    // for-in guard that react yells about if it's not here
    if (!Object.prototype.hasOwnProperty.call(field.selected, k)) {
      break;
    }
    const columnName = field.selected[k]['field_name'];
    cxt.fillText(columnName, box.x + box.padding.left, field.y + offset);

    // add to the clickables list
    var fieldObj = new Clickable(box.x, field.y+offset, box.x + box.width, field.y + offset + field.lineHeight, null)
    clickables.push(fieldObj);
    // console.log(clickables);

    // add in a seperator line, if applicable
    // 1/2 offset happens before and another after line drawing
    offset += field.lineHeight + field.lineSpace/2.0;
    if(seperators){
      var line = {}
      line.marginBottom = 3;
      line.y = field.y + offset;
      drawLine(cxt, box, line);
    }
    offset += field.lineSpace/2.0;
  }
}

// draw count of remaining columns that were not selected
function drawNumUnselectedFields(cxt, box, fieldUnselected) {
  const text = fieldUnselected.num + ' more fields...'
  cxt.fillText(text, box.x + box.padding.left, fieldUnselected.y)

  // add clickables object
  var unselectedField = new Clickable(box.x, fieldUnselected.y, box.x+box.width, fieldUnselected.y + fieldUnselected.lineHeight+fieldUnselected.lineSpace, null)
  clickables.push(unselectedField);
  // console.log(clickables)
}

// draw a toggle menu triangle
function drawTriangle(cxt, box, triangle) {
  cxt.beginPath();
  cxt.moveTo(triangle.left.x, triangle.left.y);
  cxt.lineTo(triangle.right.x, triangle.right.y);
  cxt.lineTo(triangle.bottom.x, triangle.bottom.y);
  cxt.fill();
}

// draw a border around the label
function drawRectangleBorder(cxt, box){

  cxt.strokeRect(box.x, box.y, box.width, box.height);
}

// draw a background for the label
function drawRectangleBackground(cxt, box) {
  cxt.fillRect(box.x, box.y, box.width, box.height);
}

function drawSecondaryMenu(cxt, box, triangle){
  var newBox = {}
  newBox.margin = {}
  newBox.margin.top = 5;
  newBox.margin.left = triangle.marginLeft;

  newBox.padding = {}
  newBox.padding.left = 3;
  newBox.padding.right = 3;
  newBox.padding.top = 3;
  newBox.padding.bottom = 5;

  newBox.width = 150;
  newBox.height = null;

  newBox.x = box.x + box.width + newBox.margin.left;
  newBox.y = triangle.bottom.y + newBox.margin.top;

  // edge types that can be selected
  var field = {}
  field.lineHeight = 12;
  field.fillStyle = 'black';
  field.textAlign = 'left';
  field.lineSpace = 7;
  field.selected = {1: {'field_name': 'Find similar context'}, '2': {'field_name':'Find similar content'}, '3': {'field_name': 'Find PKFK'}}; // stupid format to satisfy existing field drawer
  field.y = newBox.y + newBox.padding.top;
  field.onClick = function(){console.log('named field click')};

  // recompute box height
  newBox.height = Object.keys(field.selected).length * (field.lineHeight + field.lineSpace)-1;

  // actually start drawing things

  cxt.fillStyle = '#f2f2f2';
  drawRectangleBackground(cxt, newBox);

  cxt.strokeStyle = 'black';
  cxt.lineWidth = .5;
  drawRectangleBorder(cxt, newBox);

  cxt.fillStyle = field.fillStyle;
  cxt.font = field.lineHeight + 'px sans-serif';
  cxt.textBaseline = 'top';
  cxt.textAlign = field.textAlign;
  drawNamedFields(cxt, newBox, field, true);
}

// change the mouse to a pointer if over a clickable element
function handleMousemove(event, box, triangle){
  const overBox = inBox(box, event.layerX, event.layerY);
  const overTriangle = inTriangle(triangle, event.layerX, event.layerY);
  if(overBox || overTriangle){
    document.body.style.cursor = 'pointer';
  } else{
    document.body.style.cursor = 'default';
  }
  console.log(event.layerX, event.layerY);
}

// handle clicks inside of the box or trianglew
function handleClick(event, box, triangle, canvas){
  const clickInBox = inBox(box, event.layerX, event.layerY);
  if(clickInBox){
    console.log('click in box');
  }

  const clickInTriangle = inTriangle(triangle, event.layerX, event.layerY);
  if(clickInTriangle){
    console.log('click in triangle');
    drawSecondaryMenu(canvas.getContext('2d'), box, triangle);
  }

  // remove the canvas if not clicking the box or traingle
  if(!clickInBox && !clickInTriangle){
    canvas.parentNode.removeChild(canvas);
  }
}

// are x and y inside of the box?
function inBox(box, x, y){
  if((box.x <= x) && (x <= box.x+box.width) && (box.y <= x) && (box.y <= box.y+box.height)){
    return true;
  }
  return false;
}

// are the x and y coordinates in a triangle?
// compute the area of the triangle, and all triangles made by the combo
// of the click coordiantes and two other verticies
// see if the sum of the click triangles is the same as the triangle area
function inTriangle(triangle, x, y){
  const a = triangleArea(triangle.left.x, triangle.left.y, triangle.right.x, triangle.right.y, triangle.bottom.x, triangle.bottom.y)
  const a1 = triangleArea(triangle.left.x, triangle.left.y, triangle.right.x, triangle.right.y, x, y)
  const a2 = triangleArea(triangle.left.x, triangle.left.y, x, y, triangle.bottom.x, triangle.bottom.y)
  const a3 = triangleArea(x, y, triangle.right.x, triangle.right.y, triangle.bottom.x, triangle.bottom.y)

  if (a === a1+a2+a3){
    return true;
  }
  return false;
}

function triangleArea(x1, y1, x2, y2, x3, y3){
  return Math.abs((x1*(y2-y3) + x2*(y3-y1)+ x3*(y1-y2))/2.0);
}

function insertAfter(referenceNode, newNode) {
    referenceNode.parentNode.insertBefore(newNode, referenceNode.nextSibling);
}

function getLines(cxt, text, maxWidth){

    var characters = text.split('');
    var lines = [];
    var currentLine = characters[0];

    for (var i = 1; i < characters.length; i++) {
        var char = characters[i];
        var width = cxt.measureText(currentLine + char).width;
        if (width < maxWidth) {
            currentLine += char;
        } else {
            lines.push(currentLine);
            currentLine = char;
        }
    }
    lines.push(currentLine);
    return lines;
}