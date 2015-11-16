package graph.generators

import scala.util.Random
import graph.{ Edge, SimpleEdge, EdgeProvider }

class ErdosRenyi(scale: Int, degree: Int) extends EdgeProvider[SimpleEdge] {
  val V = 1 << scale

  def vertices = Iterator.from(0).take(V)

  def getEdges = for (u <- vertices; v <- vertices if Random.nextInt(V) < degree)
    yield Edge(u, v)
}

class ErdosRenyiSimplified(scale: Int, degree: Int) extends EdgeProvider[SimpleEdge] {
  val V = 1 << scale
  val E = V.toLong * degree
  val I = Iterator.iterate(0L)(_ + 1L).takeWhile { _ < E }

  def getEdges = for (e <- I) yield Edge(Random.nextInt(V), Random.nextInt(V))
}