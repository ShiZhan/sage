package generators

import scala.util.Random
import graph.{ Edge, EdgeProvider }

class SmallWorld(scale: Int, neighbour: Int, rewiring: Double) extends EdgeProvider[Edge] {
  require(scale > 0
    && neighbour > 0 && neighbour < (1L << (scale - 1))
    && rewiring < 1 && rewiring > 0)

  val total = 1L << scale
  val range = 1 << 20
  val probability = (range * rewiring).toInt

  def vertices = {
    var vID = -1L
    Iterator.continually { vID += 1; vID }.takeWhile(_ < total)
  }

  def neighbours(id: Long) = (1 to neighbour).toIterator.map { n =>
    if (Random.nextInt(range) < probability)
      (id + Random.nextLong) & (total - 1)
    else
      (id + n) & (total - 1)
  }

  def getEdges = for (u <- vertices; v <- neighbours(u))
    yield Edge(u, v)
}