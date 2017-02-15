// hacky little file that does things like clear the labels canvas

export function clearLabels(){
  const labelCanvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  // get the full height and width of the window
  const width = document.documentElement.clientWidth;
  const height = document.documentElement.clientHeight;

  labelCanvas.clearRect(0, 0, width, height)

}

export function drawInfoBox(sourceName, columns, x, y){
  const labelCanvas = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  const boxWidth = 250;
  const boxHeight = 200;
  const boxMarginTop = 15;

  const boxPaddingLeft = 3;
  const boxPaddingTop = 3;

  const boxX = x - boxWidth/2;
  const boxY = y + boxMarginTop;

  const lineHeight = 14; // number of pixels in a row

  var yOffset = 0; // how far offset the cursor should be

  // background rectangle
  labelCanvas.fillStyle = '#e5e5e5';
  labelCanvas.fillRect(boxX, boxY, boxWidth, boxHeight);

  // stroke around the rectangle
  labelCanvas.strokeStyle = 'black';
  labelCanvas.strokeRect(boxX, boxY, boxWidth, boxHeight);

  // table name
  labelCanvas.fillStyle = 'black';
  labelCanvas.font = 14 + 'px sans-serif';
  labelCanvas.textBaseline = 'top';
  labelCanvas.textAlign = 'center';
  const lines = getLines(labelCanvas, sourceName, boxWidth - 2*boxPaddingLeft)

  // iterate through lines
  for (var i = 0; i < lines.length; i++) {
    const line = lines[i]
    yOffset = i * lineHeight;
    labelCanvas.fillText(line, boxX + boxWidth/2, boxY + boxPaddingTop + yOffset);
  }

  // move the yOffset to below the last line
  // and add an X px margin
  yOffset += lineHeight + 9;
  console.log(yOffset);

  // draw a line at the y offset (under the last bit of source text)
  labelCanvas.beginPath();
  labelCanvas.moveTo(boxX, boxY + yOffset);
  labelCanvas.lineTo(boxX + boxWidth, boxY + yOffset);
  labelCanvas.stroke();

  // draw columns





  // columns

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