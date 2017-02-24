class Shape {
  constructor(clickable, onClick, ctx, fillStyle, strokeStyle, lineWidth){
    this.clickable = clickable; // boolean. Is this a clickable element?
    this.onClidk = onClick; // function to handle click events
    this.ctx = ctx; // HTML canvas context
    this.fillStyle = fillStyle; // ctx for fill style. HTML color as string.
    this.strokeStyle = strokeStyle; // ctx for stroke. HTML color as string.
    this.lineWidth = lineWidth; // ctx for lineWidth. number.
  }

  render () {
    console.err('Shape.render() is an abstract method.')
  }

  inShape (x, y) {
    console.err('Shape.inShape() is an abstractMethod.')
  }
}

class Box extends Shape {
  constructor(clickable, onClick, ctx, fillStyle, strokeStyle, lineWidth, font, textAlign, coordinates, border, text) {
    super(clickable, onClick, ctx, fillStyle, strokeStyle, lineWidth);
    this.font = font; // ctx font as string. e.g. 12px sans-serif
    this.textAlign = textAlign; // ctx for text alignment. left right center;
    this.c = coordinates; // object with keys x1, y1, x2, y2 and number values.
    this.border = border; // object with keys top, left, right, bottom and values bool
    this.text = text; // string to be written

  }

  get width(){
    return Math.abs(this.c.x2 - this.c.x1);
  }

  get height(){
    return Math.abs(this.c.y2 - this.c.y1);
  }

  computeHeight(){
    // get the number of lines and infer and set the height of the object
    this.height = null;
  }

  get lines(){
    // return an array of lines that fits the given width
  }

  inShape(x, y){
    // is the provided point in the shape? returns bool
    if((this.c.x1 <= x) && (x <= this.c.x2) && (this.c.y1 <= y) && (y <= this.c.y2)){
      return true;
    }
    return false;
  }

  render(){
    // set canvas variables
    this.ctx.fillStyle = this.fillStyle;
    this.ctx.strokeStyle = this.strokeStyle;
    this.ctx.lineWidth = this.lineWidth;
    this.ctx.font = this.font;
    this.ctx.textAlign = this.textAlign

    // render background
    // debugger;
    this.ctx.fillRect(this.c.x1, this.c.y1, this.width, this.height);

    // render text

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
  constructor(clickable, onClick, ctx, fillStyle, strokeStyle, lineWidth, coordinates) {
    super(clickable, onClick, ctx, fillStyle, strokeStyle, lineWidth);
    this.c = coordinates; // object with keys x1 y1 x2 y2, x3 y3 and number values
  }

  inShape(x, y) {
    // is the pr  :320ovided point in the shape? returns bool

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
    this.ctx.fillStyle = this.fillStyle;
    this.ctx.strokeStyle = this.strokeStyle;
    this.ctx.lineWidth = this.lineWidth;

    this.ctx.beginPath();
    this.ctx.moveTo(this.x1, this.y1);
    this.ctx.lineTo(this.x2, this.y2);
    this.ctx.lineTo(this.x3, this.y3);
    this.ctx.fill();
  }


}

export function renderCanvas(source, columnsSelected, columnsAll, x, y){
  // get the sigma-mouse canvas, clone and modify
  const mouseCanvas = document.getElementsByClassName('sigma-mouse')[0]
  const newCanvas = cloneCanvasAndInsertAbove(mouseCanvas);
  const ctx = newCanvas.getContext('2d');

  // var coords = {x1: 0, y1:0, x2: 100, y2:200}
  // var border = {top: true, right: true, bottom:true, left: true}
  // var mainMenu = new Box(false, null, ctx, 'orange', 'green', 1, '12px sans-serif', 'center', coords, border, 'footext')
  // mainMenu.render();

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
  return newCanvas;
}

function insertAfter(referenceNode, newNode) {
    referenceNode.parentNode.insertBefore(newNode, referenceNode.nextSibling);
}