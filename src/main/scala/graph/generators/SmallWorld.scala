package graph.generators

import scala.util.Random
import graph.{ Edge, SimpleEdge, EdgeProvider }

class SmallWorld(scale: Int, neighbour: Int, rewiring: Double) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 63
    && neighbour > 0 && neighbour < (1L << (scale - 1))
    && rewiring < 1 && rewiring > 0)

  val V = 1L << scale
  val M = V - 1
  val R = 1 << 30
  val P = (R * rewiring).toInt

  def vertices = Iterator.iterate(0L)(_ + 1L).takeWhile { _ < V }

  def neighbours(id: Long) = Iterator.iterate(id + 1)(_ + 1).take(neighbour)
    .map { n => if (n > M) n & M else n }

  def getEdges = for (u <- vertices; v <- neighbours(u))
    yield if (Random.nextInt(R) > P) Edge(u, v) else Edge(u, (v + Random.nextLong) & M)
}
