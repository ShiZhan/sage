package graph.generators

import scala.util.Random
import graph.{ Edge, SimpleEdge, EdgeProvider }

class SmallWorld(scale: Int, neighbour: Int, rewiring: Double) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 31
    && neighbour > 0 && neighbour < (1L << (scale - 1))
    && rewiring < 1 && rewiring > 0)

  val total = 1 << scale
  val range = 1 << 20
  val probability = (range * rewiring).toInt

  def vertices = Iterator.from(0).take(total)

  def neighbours(id: Int) = Iterator.from(1).take(neighbour).map { n =>
    {
      if (Random.nextInt(range) < probability)
        id + Random.nextInt(total)
      else
        id + n
    } & (total - 1)
  }

  def getEdges = for (u <- vertices; v <- neighbours(u))
    yield Edge(u, v)
}