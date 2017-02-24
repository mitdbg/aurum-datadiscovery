class Shape {
  constructor(clickable, onClick, cxt, fillStyle, strokeStyle, lineWidth){
    this.clickable = clickable; // boolean. Is this a clickable element?
    this.onClidk = onClick; // function to handle click events
    this.cxt = cxt; // HTML canvas context
    this.fillStyle = fillStyle; // cxt for fill style. HTML color as string.
    this.strokeStyle = strokeStyle; // cxt for stroke. HTML color as string.
    this.lineWidth = lineWidth; // cxt for lineWidth. number.
  }

  render () {
    console.err('Shape.render() is an abstract method.')
  }

  inShape (x, y) {
    console.err('Shape.inShape() is an abstractMethod.')
  }
}

class Box extends Shape {
  constructor(clickable, onClick, cxt, fillStyle, strokeStyle, lineWidth, font, textAlign, coordinates, border, text) {
    super(clickable, onClick, cxt, fillStyle, strokeStyle, lineWidth);
    this.font = font; // cxt font as string. e.g. 12px sans-serif
    this.textAlign = textAlign; // cxt for text alignment. left right center;
    this.c = coordinates; // object with keys x1, y1, x2, y2 and number values.
    this.border = border; // object with keys top, left, right, bottom and values bool
    this.text = text; // string to be written

  }

  get width(){
    return x1 - x2;
  }

  get height(){
    return y1 - y2;
  }

  set height(){
    // get the number of lines and infer and set the height of the object
    this.height = null;
  }

  get lines(){
    // return an array of lines that fits the given width
  }

  inShape(x, y){
    // is the provided point in the shape? returns bool
    if((c.x1 <= x) && (x <= c.x2) && (c.y1 <= y) && (y <= c.y2)){
      return true;
    }
    return false;
  }

  render(){
    // render background

    // render text

    // render borders
  }
}

class Triangle extends Shape {
  constructor(clickable, onClick, cxt, fillStyle, strokeStyle, lineWidth, coordinates) {
    super(clickable, onClick, cxt, fillStyle, strokeStyle, lineWidth);
    this.c = coordinates; // object with keys x1 y1 x2 y2, x3 y3 and number values
  }

  inShape(x, y) {
    // is the provided point in the shape? returns bool

    // compute the area of the shape
    const a = triangleArea(this.c.x1, this.c.y1, this.c.x2, this.c.y2, this.c.x3, this.c.y3);

    // compute the area of all triangles made by two of the shapes verticies and the point
    const a1 = triangleArea(x, y, this.c.x2, this.c.y2, this.c.x3, this.c.y3);
    const a2 = triangleArea(this.c.x1, this.c.y1, x, y, this.c.x3, this.c.y3);
    const a3 = triangleArea(this.c.x1, this.c.y1, this.c.x2, this.c.y2, x, y);

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
    // render background
  }


}
