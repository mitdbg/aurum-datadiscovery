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

  // line
  var line = {}
  line.marginBottom = 2;

  // column
  var field = {}
  field.lineHeight = 12;
  field.fillStyle = 'black'
  field.textAlign = 'left';
  field.lineSpace = 2;
  field.selected = selectedColumns;
  field.numUnselected = Object.keys(allColumns).length - Object.keys(field.selected).length



  // offset variable, which needs to die
  var offset = {}
  offset.y = 0;

  offset = drawSource(canvas, box, offset, source);

  var numLines = getLines(canvas, source.name, box.width - box.padding.left - box.padding.right).length
  box.height = numLines * (source.lineHeight + source.lineSpace) + source.marginBottom;

  console.log(offset.y)
  console.log(box.height)

  offset = drawLine(canvas, box, offset, line)
  box.height = numLines * (source.lineHeight + source.lineSpace) + source.marginBottom + box.padding.top;
  console.log(offset.y)
  console.log(box.height)

  offset = drawSelectedFields(canvas, box, offset, field);
  // box.height = numLines * (source.lineHeight + source.lineSpace) + source.marginBottom + box.padding.top + ;
  // console.log(offset.y)
  // console.log(box.height)


  offset = drawNumUnselectedColumns(canvas, box, offset, field);

  // drawRectangleBackground(canvas, box, offset);
  drawRectangleBorder(canvas, box, offset);

}

// draw the table name
function drawSource(canvas, box, offset, source) {
  canvas.fillStyle = source.fillStyle;
  canvas.font = source.lineHeight + 'px sans-serif';
  canvas.textBaseline = 'top';
  canvas.textAlign = source.textAlign;
  const lines = getLines(canvas, source.name, box.width - box.padding.left - box.padding.right)

  // iterate through lines
  for (var i = 0; i < lines.length; i++) {
    const line = lines[i]
    offset.y = offset.y * source.lineHeight;
    canvas.fillText(line, box.x + box.width/2, box.y + box.padding.top + offset.y);
  }

  offset.y += source.lineHeight + source.marginBottom;
  return offset
}

// draw a horizontal line
function drawLine(canvas, box, offset, line) {
  canvas.beginPath();
  canvas.moveTo(box.x, box.y + offset.y);
  canvas.lineTo(box.x + box.width, box.y + offset.y);
  canvas.stroke();
  offset.y += line.marginBottom;

  return offset;
}

// draw the columns that were selected
function drawSelectedFields(canvas, box, offset, field){
  canvas.fillStyle = field.fillStyle;
  canvas.font = field.lineHeight + 'px sans-serif';
  canvas.textBaseline = 'top';
  canvas.textAlign = field.textAlign;

  for (var k in field.selected){

    // for-in guard that react yells about if it's not here
    if (!Object.prototype.hasOwnProperty.call(field.selected, k)) {
      break;
    }
    const columnName = field.selected[k]['field_name'];
    canvas.fillText(columnName, box.x + box.padding.left, box.y + box.padding.top + offset.y);
    offset.y += field.lineHeight;
  }

  return offset;
}

// draw count of remaining columns that were not selected
function drawNumUnselectedColumns(canvas, box, offset, field) {

  const text = field.numUnselected + ' more fields...'
  canvas.fillStyle = 'gray';
  canvas.fillText(text, box.x + box.padding.left, box.y + box.padding.top + offset.y)
  offset.y += field.lineHeight + box.padding.top

  return offset;
}

// draw a border around the label
function drawRectangleBorder(canvas, box, offset){
  canvas.strokeStyle = 'black';
  canvas.strokeRect(box.x, box.y, box.width, offset.y);
}

// draw a background for the label
function drawRectangleBackground(canvas, box) {
  canvas.fillStyle = '#e5e5e5';
  canvas.fillRect(box.x, box.y, box.width, box.y + box.height);
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