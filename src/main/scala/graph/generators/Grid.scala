package graph.generators

import graph.{ Edge, SimpleEdge, EdgeProvider }

class Grid2(xScale: Int, yScale: Int) extends EdgeProvider[SimpleEdge] {
  require(xScale > 0 && xScale < 30 && yScale > 0 && yScale < 30)

  val X = 1 << xScale
  val Y = 1 << yScale

  def sequence(size: Int) = Iterator.from(0).take(size)
  def id(x: Int, y: Int) = (x << yScale) + y

  def getEdges = for (
    x <- sequence(X);
    y <- sequence(Y);
    u = id(x, y);
    v <- Iterator(id(x + 1 & X - 1, y), id(x, y + 1 & Y - 1))
  ) yield Edge(u, v)
}

class Grid3(xScale: Int, yScale: Int, zScale: Int) extends EdgeProvider[SimpleEdge] {
  require(xScale > 0 && xScale < 20 && yScale > 0 && yScale < 20 && zScale > 0 && zScale < 20)

  val X = 1 << xScale
  val Y = 1 << yScale
  val Z = 1 << zScale

  def sequence(size: Int) = Iterator.from(0).take(size)
  def id(x: Int, y: Int, z: Int) = (x << (yScale + zScale)) + (y << zScale) + z

  def getEdges = for (
    x <- sequence(X);
    y <- sequence(Y);
    z <- sequence(Z);
    u = id(x, y, z);
    v <- Iterator(id(x + 1 & X - 1, y, z), id(x, y + 1 & Y - 1, z), id(x, y, z + 1 & Z - 1))
  ) yield Edge(u, v)
}