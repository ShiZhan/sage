package generators

import graph.Edge

class Grid2(rScale: Int, cScale: Int) {
  require(rScale > 0 && rScale < 30 && cScale > 0 && cScale < 30)
  val row = 1L << rScale
  val col = 1L << cScale

  def sequence(size: Long) = {
    var i = -1L
    Iterator.continually { i += 1; i }.takeWhile(_ < size)
  }

  def getIterator =
    sequence(row).flatMap { r =>
      sequence(col).flatMap { c =>
        val id = (r << cScale) + c
        val idH = (r << cScale) + (c + 1 & col - 1)
        val idV = (((r + 1) & (row - 1)) << cScale) + c
        Iterator(Edge(id, idH), Edge(id, idV))
      }
    }
}

class Grid3(xScale: Int, yScale: Int, zScale: Int) {
  require(xScale > 0 && xScale < 20
    && yScale > 0 && yScale < 20
    && zScale > 0 && zScale < 20)

  val X = 1L << xScale
  val Y = 1L << yScale
  val Z = 1L << zScale

  def sequence(size: Long) = {
    var i = -1L
    Iterator.continually { i += 1; i }.takeWhile(_ < size)
  }

  def getIterator =
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