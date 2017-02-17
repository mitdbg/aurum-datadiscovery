// hacky little file that does things like clear the labels cxt

export function clearLabels(){
  const labelCanvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  // get the full height and width of the window
  const width = document.documentElement.clientWidth;
  const height = document.documentElement.clientHeight;

  labelCanvas.clearRect(0, 0, width, height)

}

export function drawInfoBox(sourceName, selectedColumns, allColumns, x, y){

  // get the sigma-mouse canvas
  const mouseCanvas = document.getElementsByClassName('sigma-mouse')[0]

  // clone it and modify
  var newCanvas = mouseCanvas.cloneNode(true);
  newCanvas.className = 'aurum-overlay';
  newCanvas.id = 'aurum-overlay';
  const cxt = newCanvas.getContext('2d');

  // assume everything is retna for now, and scale accordingly
  cxt.scale(2,2);

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
  field.fillStyle = 'black'
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


  drawRectangleBackground(cxt, box);
  drawRectangleBorder(cxt, box);
  drawTriangle(cxt, box, triangle);

  drawSource(cxt, box, source);
  drawLine(cxt, box, line);
  drawSelectedFields(cxt, box, field);
  drawNumUnselectedFields(cxt, box, fieldUnselected);

  // add an event handler
  newCanvas = document.getElementById('aurum-overlay')
  newCanvas.addEventListener('click', (event)=>remove(event, newCanvas));

}

// draw the table name
function drawSource(cxt, box, source) {
  cxt.fillStyle = source.fillStyle;
  cxt.font = source.lineHeight + 'px sans-serif';
  cxt.textBaseline = 'top';
  cxt.textAlign = source.textAlign;

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
function drawSelectedFields(cxt, box, field){
  cxt.fillStyle = field.fillStyle;
  cxt.font = field.lineHeight + 'px sans-serif';
  cxt.textBaseline = 'top';
  cxt.textAlign = field.textAlign;

  var offset = 0;
  for (var k in field.selected){

    // for-in guard that react yells about if it's not here
    if (!Object.prototype.hasOwnProperty.call(field.selected, k)) {
      break;
    }
    const columnName = field.selected[k]['field_name'];
    cxt.fillText(columnName, box.x + box.padding.left, field.y + offset);
    offset += field.lineHeight + field.lineSpace;
  }

}

// draw count of remaining columns that were not selected
function drawNumUnselectedFields(cxt, box, fieldUnselected) {
  const text = fieldUnselected.num + ' more fields...'
  cxt.fillStyle = 'gray';
  cxt.fillText(text, box.x + box.padding.left, fieldUnselected.y)
}

function drawTriangle(cxt, box, triangle) {
  cxt.fillStyle = triangle.fillStyle;
  cxt.beginPath();
  cxt.moveTo(triangle.left.x, triangle.left.y);
  cxt.lineTo(triangle.right.x, triangle.right.y);
  cxt.lineTo(triangle.bottom.x, triangle.bottom.y);
  cxt.fill();

}

// draw a border around the label
function drawRectangleBorder(cxt, box, offset){
  cxt.strokeStyle = 'black';
  cxt.strokeRect(box.x, box.y, box.width, box.height);
}

// draw a background for the label
function drawRectangleBackground(cxt, box) {
  cxt.fillStyle = '#f2f2f2';
  cxt.fillRect(box.x, box.y, box.width, box.height);
}

function remove(event, canvas){
  console.log('aurum UI event');
  console.log(event);
  canvas.parentNode.removeChild(canvas);
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