// hacky little file that does things like clear the labels canvas

export function clearLabels(){
  const labelCanvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  // get the full height and width of the window
  const width = document.documentElement.clientWidth;
  const height = document.documentElement.clientHeight;

  labelCanvas.clearRect(0, 0, width, height)

}

export function drawInfoBox(sourceName, selectedColumns, allColumns, x, y){
  const canvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');

  var box = {}

  box.margin = {}
  box.margin.top = 15;

  box.padding = {}
  box.padding.left = 3;
  box.padding.right = 3;
  box.padding.top = 3;
  box.padding.bottom = 5;

  box.width = 250;
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
  source.lines = getLines(canvas, source.name, box.width - box.padding.left - box.padding.right)
  source.y = box.padding.top + box.y;

  // line
  var line = {}
  line.marginBottom = 2;
  line.y = source.lines.length * (source.lineHeight + source.lineSpace) + source.marginBottom + source.y

  // column selected
  var field = {}
  field.lineHeight = 12;
  field.fillStyle = 'black'
  field.textAlign = 'left';
  field.lineSpace = 2;
  field.selected = selectedColumns;
  field.y = line.y + line.marginBottom;

  var fieldUnselected = {}
  fieldUnselected.lineHeight = 12;
  fieldUnselected.fillStyle = 'black'
  fieldUnselected.textAlign = 'left';
  fieldUnselected.lineSpace = 2;
  fieldUnselected.num = Object.keys(allColumns).length - Object.keys(field.selected).length
  fieldUnselected.y = Object.keys(field.selected).length * (field.lineHeight + field.lineSpace) + field.y

  box.height = fieldUnselected.y - box.y + fieldUnselected.lineHeight + fieldUnselected.lineSpace;


  drawRectangleBackground(canvas, box);
  drawRectangleBorder(canvas, box);

  drawSource(canvas, box, source);
  drawLine(canvas, box, line);
  drawSelectedFields(canvas, box, field);
  drawNumUnselectedFields(canvas, box, fieldUnselected);


}

// draw the table name
function drawSource(canvas, box, source) {
  canvas.fillStyle = source.fillStyle;
  canvas.font = source.lineHeight + 'px sans-serif';
  canvas.textBaseline = 'top';
  canvas.textAlign = source.textAlign;

  // iterate through lines
  var offset = 0;
  for (var i = 0; i < Object.keys(source.lines).length; i++) {
    const line = source.lines[i]
    canvas.fillText(line, box.x + box.width/2, source.y + offset);
    offset += source.lineHeight + source.lineSpace;
  }
}

// draw a horizontal line
function drawLine(canvas, box, line) {
  canvas.beginPath();
  canvas.moveTo(box.x, line.y);
  canvas.lineTo(box.x + box.width, line.y);
  canvas.stroke();
}

// draw the columns that were selected
function drawSelectedFields(canvas, box, field){
  canvas.fillStyle = field.fillStyle;
  canvas.font = field.lineHeight + 'px sans-serif';
  canvas.textBaseline = 'top';
  canvas.textAlign = field.textAlign;

  var offset = 0;
  for (var k in field.selected){

    // for-in guard that react yells about if it's not here
    if (!Object.prototype.hasOwnProperty.call(field.selected, k)) {
      break;
    }
    const columnName = field.selected[k]['field_name'];
    canvas.fillText(columnName, box.x + box.padding.left, field.y + offset);
    offset += field.lineHeight + field.lineSpace;
  }

}

// draw count of remaining columns that were not selected
function drawNumUnselectedFields(canvas, box, fieldUnselected) {
  const text = fieldUnselected.num + ' more fields...'
  canvas.fillStyle = 'gray';
  canvas.fillText(text, box.x + box.padding.left, fieldUnselected.y)
}

// draw a border around the label
function drawRectangleBorder(canvas, box, offset){
  canvas.strokeStyle = 'black';
  canvas.strokeRect(box.x, box.y, box.width, box.height);
}

// draw a background for the label
function drawRectangleBackground(canvas, box) {
  canvas.fillStyle = '#e5e5e5';
  canvas.fillRect(box.x, box.y, box.width, box.height);
}


function getLines(canvas, text, maxWidth) {
    var characters = text.split('');
    var lines = [];
    var currentLine = characters[0];

    for (var i = 1; i < characters.length; i++) {
        var char = characters[i];
        var width = canvas.measureText(currentLine + char).width;
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