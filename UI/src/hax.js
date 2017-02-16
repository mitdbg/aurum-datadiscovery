// hacky little file that does things like clear the labels canvas

export function clearLabels(){
  const labelCanvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  // get the full height and width of the window
  const width = document.documentElement.clientWidth;
  const height = document.documentElement.clientHeight;

  labelCanvas.clearRect(0, 0, width, height)

}

export function drawInfoBox(sourceName, selectedColumns, allColumns, x, y){
  const labelCanvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  const boxWidth = 250;
  const boxHeight = 200;
  const boxMarginTop = 15;

  const boxPaddingLeft = 3;
  const boxPaddingTop = 3;

  const boxX = x - boxWidth/2;
  const boxY = y + boxMarginTop;

  const sourceLineHeight = 14; // number of pixels in a row
  const fieldLineHeight = 12; // number of pixels in a row

  var yOffset = 0; // how far offset the cursor should be

  // draw table name
  labelCanvas.fillStyle = 'black';
  labelCanvas.font = sourceLineHeight + 'px sans-serif';
  labelCanvas.textBaseline = 'top';
  labelCanvas.textAlign = 'center';
  const lines = getLines(labelCanvas, sourceName, boxWidth - 2*boxPaddingLeft)

  // iterate through lines
  for (var i = 0; i < lines.length; i++) {
    const line = lines[i]
    yOffset = i * sourceLineHeight;
    labelCanvas.fillText(line, boxX + boxWidth/2, boxY + boxPaddingTop + yOffset);
  }

  // move the yOffset to below the last line
  // and add an X px margin
  yOffset += sourceLineHeight + 9;
  // console.log(yOffset);

  // draw a line at the y offset (under the last bit of source text)
  labelCanvas.beginPath();
  labelCanvas.moveTo(boxX, boxY + yOffset);
  labelCanvas.lineTo(boxX + boxWidth, boxY + yOffset);
  labelCanvas.stroke();

  // move yOffset down again
  yOffset += 2;

  // draw columns
  labelCanvas.fillStyle = 'black';
  labelCanvas.font = fieldLineHeight + 'px sans-serif';
  labelCanvas.textBaseline = 'top';
  labelCanvas.textAlign = 'left';

  for (var k in selectedColumns){

    // for-in guard that react yells about if it's not here
    if (!Object.prototype.hasOwnProperty.call(selectedColumns, k)) {
      break;
    }
    const columnName = selectedColumns[k]['field_name'];
    labelCanvas.fillText(columnName, boxX + boxPaddingLeft, boxY + boxPaddingTop + yOffset);
    yOffset += sourceLineHeight;
  }

  // number of remaining columns to be displayed
  const colRemainNum = Object.keys(allColumns).length - Object.keys(selectedColumns).length
  const text = colRemainNum + ' more fields...'
  labelCanvas.fillStyle = 'gray';
  labelCanvas.fillText(text, boxX + boxPaddingLeft, boxY + boxPaddingTop + yOffset)
  yOffset += sourceLineHeight + boxPaddingTop

  // background rectangle
  // labelCanvas.fillStyle = '#e5e5e5';
  // labelCanvas.fillRect(boxX, boxY, boxWidth, yOffset);

  // draw a rectangle around the box
  labelCanvas.strokeStyle = 'black';
  labelCanvas.strokeRect(boxX, boxY, boxWidth, yOffset);



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