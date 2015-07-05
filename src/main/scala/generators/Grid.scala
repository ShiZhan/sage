package generators

import graph.{ Edge, SimpleEdge, EdgeProvider }

class Grid2(rScale: Int, cScale: Int) extends EdgeProvider[SimpleEdge] {
  require(rScale > 0 && rScale < 30 && cScale > 0 && cScale < 30)
  val row = 1 << rScale
  val col = 1 << cScale

  def sequence(size: Int) = Iterator.from(0).take(size)

  def getEdges =
    sequence(row).flatMap { r =>
      sequence(col).flatMap { c =>
        val id = (r << cScale) + c
        val idH = (r << cScale) + (c + 1 & col - 1)
        val idV = (((r + 1) & (row - 1)) << cScale) + c
        Iterator(Edge(id, idH), Edge(id, idV))
      }
    }
}

class Grid3(xScale: Int, yScale: Int, zScale: Int) extends EdgeProvider[SimpleEdge] {
  require(xScale > 0 && xScale < 20
    && yScale > 0 && yScale < 20
    && zScale > 0 && zScale < 20)

  val X = 1 << xScale
  val Y = 1 << yScale
  val Z = 1 << zScale

  def sequence(size: Int) = Iterator.from(0).take(size)

  def getEdges =
    sequence(X).flatMap { x =>
      sequence(Y).flatMap { y =>
        sequence(Z).flatMap { z =>
          val id = (x << (yScale + zScale)) + (y << zScale) + z
          val idX = ((x + 1 & X - 1) << (yScale + zScale)) + (y << zScale) + z
          val idY = (x << (yScale + zScale)) + ((y + 1 & Y - 1) << zScale) + z
          val idZ = (x << (yScale + zScale)) + (y << zScale) + (z + 1 & Z - 1)
          Iterator(Edge(id, idX), Edge(id, idY), Edge(id, idZ))
        }
      }
    }
}