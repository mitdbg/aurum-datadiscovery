var shapes = [];

class Shape {
  constructor(clickable, onClick, ctx, bkgFillStyle, strokeStyle, lineWidth){
    this.clickable = clickable; // boolean. Is this a clickable element?
    this.onClidk = onClick; // function to handle click events
    this.ctx = ctx; // HTML canvas context
    this.bkgFillStyle = bkgFillStyle; // ctx for fill style. HTML color as string.
    this.strokeStyle = strokeStyle; // ctx for stroke. HTML color as string.
    this.lineWidth = lineWidth; // ctx for lineWidth. number.
    shapes.push(this)
  }

  render () {
    console.err('Shape.render() is an abstract method.')
  }

  inShape (x, y) {
    console.err('Shape.inShape() is an abstractMethod.')
  }
}

class Box extends Shape {
  constructor(clickable, onClick, ctx, bkgFillStyle, strokeStyle, lineWidth, fontStyle, fontWeight, textAlign, coordinates, bkg, border, text, textHeight, textColor) {
    super(clickable, onClick, ctx, bkgFillStyle, strokeStyle, lineWidth);
    this.fontStyle = fontStyle; // font style as string. sans-serif
    this.fontWeight = fontWeight; // weight as string. bold normal
    this.textAlign = textAlign; // ctx for text alignment. left right center;
    this.c = coordinates; // object with keys x1, y1, x2, y2 and number values.
    this.bkg = bkg; // should background be drawn? bool.
    this.border = border; // object with keys top, left, right, bottom and values bool
    this.text = text; // string to be written
    this.textHeight = textHeight; // integer. e.g. 12
    this.textColor = textColor; // HTML color for ctx

  }

  get font(){
    return this.fontWeight + ' ' + this.textHeight.toString() + 'px ' + this.fontStyle;
  }

  get width(){
    return Math.abs(this.c.x2 - this.c.x1);
  }

  get height(){
    return Math.abs(this.c.y2 - this.c.y1);
  }

  get lines(){
    // return an array of lines that fits the given width, as computed in get width()
    const characters = this.text.split('');
    var lines = [];
    var currentLine = characters[0];

    for (var i = 1; i < characters.length; i++) {
      var char = characters[i];
      var width = this.ctx.measureText(currentLine + char).width;
      if (width < this.width) {
        currentLine += char;
      } else {
        lines.push(currentLine);
        currentLine = char;
      }
    }

    // make sure we don't return an arary [undefined]
    if (currentLine !== undefined && currentLine !== null){
      lines.push(currentLine);
    }
    return lines;
  }

  computeHeight(){
    // get the number of lines and infer and set the height of the object
    // this resets the y2 variable.
    // 1.6 is just a ballpark that happens to work
    // there's not much rhyme or reason to it. :/
    this.c.y2 = this.c.y1 + this.lines.length * this.textHeight * 1.5;
  }

  inShape(x, y){
    // is the provided point in the shape? returns bool
    if((this.c.x1 <= x) && (x <= this.c.x2) && (this.c.y1 <= y) && (y <= this.c.y2)){
      return true;
    }
    return false;
  }

  render(){
    // recompute the y2 variable
    if(this.text !== null && this.text !=''){
      this.computeHeight();
    }

    // set canvas variables
    this.ctx.fillStyle = this.bkgFillStyle;
    this.ctx.strokeStyle = this.strokeStyle;
    this.ctx.lineWidth = this.lineWidth;
    this.ctx.font = this.font;

    // render background
    if (this.bkg){
      this.ctx.fillRect(this.c.x1, this.c.y1, this.width, this.height);
    }

    // render text
    this.ctx.fillStyle = this.textColor;
    this.ctx.textAlign = this.textAlign

    var offsetY = this.textHeight; // how much farther down should each line of text go?
    var offsetX = 0; // deals with text align center
    if (this.textAlign.toLowerCase() === 'center'){
      offsetX += this.width/2
    }
    for (var i = 0; i < this.lines.length; i++) {
      var l = this.lines[i];
      this.ctx.fillText(l, this.c.x1 + offsetX, this.c.y1 + offsetY);
      offsetY += this.textHeight;
    }

    // render borders
    this.ctx.beginPath()
    if (this.border.top) {
      this.ctx.moveTo(this.c.x1, this.c.y1);
      this.ctx.lineTo(this.c.x2, this.c.y1);
      this.ctx.stroke();}

    if (this.border.right) {
      this.ctx.moveTo(this.c.x2, this.c.y1);
      this.ctx.lineTo(this.c.x2, this.c.y2);
      this.ctx.stroke();}

    if (this.border.bottom) {
      this.ctx.moveTo(this.c.x2, this.c.y2);
      this.ctx.lineTo(this.c.x1, this.c.y2);
      this.ctx.stroke();}

    if (this.border.left) {
      this.ctx.moveTo(this.c.x1, this.c.y2);
      this.ctx.lineTo(this.c.x1, this.c.y1);
      this.ctx.stroke();}

  }
}

class Triangle extends Shape {
  constructor(clickable, onClick, ctx, bkgFillStyle, strokeStyle, lineWidth, coordinates, bkg) {
    super(clickable, onClick, ctx, bkgFillStyle, strokeStyle, lineWidth);
    this.c = coordinates; // object with keys x1 y1 x2 y2, x3 y3 and number values
    this.bkg = bkg; // should the background be drawn? bool.
  }

  inShape(x, y) {
    // is the provided point in the shape? returns bool

    // compute the area of the shape
    const a = this._triangleArea(this.c.x1, this.c.y1, this.c.x2, this.c.y2, this.c.x3, this.c.y3);

    // compute the area of all triangles made by two of the shapes verticies and the point
    const a1 = this._triangleArea(x, y, this.c.x2, this.c.y2, this.c.x3, this.c.y3);
    const a2 = this._triangleArea(this.c.x1, this.c.y1, x, y, this.c.x3, this.c.y3);
    const a3 = this._triangleArea(this.c.x1, this.c.y1, this.c.x2, this.c.y2, x, y);

    // if sum of the smaller triangles equals the area, it's in the shape
    if (a === a1+a2+a3){
      return true;
    }
    return false;
  }

  _triangleArea(x1, y1, x2, y2, x3, y3){
    // the area of the triangle created by the three points
    return Math.abs((x1*(y2-y3) + x2*(y3-y1)+ x3*(y1-y2))/2.0);
  }

  render(){
    this.ctx.fillStyle = this.bkgFillStyle;
    this.ctx.strokeStyle = this.strokeStyle;
    this.ctx.lineWidth = this.lineWidth;

    this.ctx.beginPath();
    this.ctx.moveTo(this.c.x1, this.c.y1);
    this.ctx.lineTo(this.c.x2, this.c.y2);
    this.ctx.lineTo(this.c.x3, this.c.y3);
    this.ctx.lineTo(this.c.x1, this.c.y1);
    this.ctx.lineTo(this.c.x2, this.c.y1); // extra stroke to deal with pointed triangle vertex
    this.ctx.stroke()

    if (this.bkg){
      this.ctx.fill();
    }
  }
}

export function renderCanvas(source, columnsSelected, columnsAll, x, y){
  // get the sigma-mouse canvas, clone and modify
  const mouseCanvas = document.getElementsByClassName('sigma-mouse')[0]
  const newCanvas = cloneCanvasAndInsertAbove(mouseCanvas);
  const ctx = newCanvas.getContext('2d');


  // create the background box when nodes are clicked
  var width = 225;
  var height = 200;
  var margin = {top: 15};
  var border = {top: true, right: true, bottom:true, left: true};
  var coords = {x1: x - width/2, y1: y + margin.top, x2: x + width/2, y2: height + y + margin.top}; // y2 isn't clear yet.

  var bkgrndBox = new Box(true, null, ctx, 'orange', 'green', 1, 'sans-serif', 'bold', 'center', coords, true, border, '', 12, 'black')


  // create the source title
  width = bkgrndBox.c.width - 5*2;
  margin = {top: 5, right: 5, bottom: 5, left: 5}
  border = {top: false, right: false, bottom: false, left:false};
  coords = {x1: bkgrndBox.c.x1 + margin.left, y1:bkgrndBox.c.y1 + margin.top, x2: bkgrndBox.c.x2 - margin.right, y2: bkgrndBox.c.y2 + 20}; // again, y2 isn't clear yet
  var sourceBox = new Box(true, null, ctx, 'lightblue', 'green', 1, 'sans-serif', 'bold', 'center', coords, true, border, source, 12, 'black')
  sourceBox.computeHeight();


  bkgrndBox.c.y2 = sourceBox.c.y2+5;

  bkgrndBox.render();
  sourceBox.render();



  // var coords = {x1: 20, y1:20, x2: 200, y2:20}
  // var border = {top: true, right: true, bottom:true, left: true}
  // const text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit."
  // var mainMenu = new Box(true, null, ctx, 'orange', 'green', 1, 'sans-serif', 'bold', 'center', coords, border, text, 12, 'black')
  // mainMenu.render();


  coords = {x1: 130, y1: 130, x2: 160, y2: 130, x3: 145, y3: 160}
  var triangle = new Triangle(true, null, ctx, 'red', 'black', 4, coords, true);
  triangle.render()

  console.log(shapes);

}

export function removeCanvas(){
  const ctx = document.getElementsByClassName('sigma-labels')[0].getContext('2d');
  // get the full height and width of the window
  const width = document.documentElement.clientWidth;
  const height = document.documentElement.clientHeight;

  ctx.clearRect(0, 0, width, height)
}

/////// /////// ///////
// helper functions //
///// /////// ///////

function cloneCanvasAndInsertAbove(oldCanvas){
  const newCanvas = oldCanvas.cloneNode(true);
  newCanvas.className = 'aurum-overlay';
  newCanvas.id = 'aurum-overlay';

  const ctx = newCanvas.getContext('2d');
  const scale = window.devicePixelRatio;
  ctx.scale(scale, scale);
  insertAfter(oldCanvas, newCanvas);
  newCanvas.addEventListener('mousemove', (event)=>handleMousemove(event));
  return newCanvas;
}

function handleMousemove(event){
  const x = event.layerX;
  const y = event.layerY;
  for (var i = 0; i < shapes.length; i++) {
    if(shapes[i].clickable && shapes[i].inShape(x, y)){
      // condition is entered, but cursor style not changed. Not sure why.
      document.body.style.cursor = 'pointer';
      break;
    }
  }
  document.body.style.cursor = 'default';
}

function insertAfter(referenceNode, newNode) {
    referenceNode.parentNode.insertBefore(newNode, referenceNode.nextSibling);
}