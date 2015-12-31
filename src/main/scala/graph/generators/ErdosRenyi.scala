package graph.generators

import scala.util.Random
import graph.{ Edge, SimpleEdge, EdgeProvider }

class ErdosRenyi(scale: Int, degree: Int) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 63 && degree > 0)

  val V = 1L << scale
  val M = V - 1
  def vertices = Iterator.iterate(0L)(_ + 1L).takeWhile { _ < V }

  def getEdges = for (u <- vertices; v <- vertices if (Random.nextLong & M) < degree)
    yield Edge(u, v)
}

class ErdosRenyiSimplified(scale: Int, degree: Int) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 63 && degree > 0)

  val V = 1L << scale
  val E = V * degree
  val M = V - 1
  val I = Iterator.iterate(0L)(_ + 1L).takeWhile { _ < E }

  def getEdges = for (e <- I) yield Edge(Random.nextLong & M, Random.nextLong & M)
}
