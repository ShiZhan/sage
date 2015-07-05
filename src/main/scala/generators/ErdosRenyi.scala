package generators

import scala.util.Random
import graph.{ Edge, SimpleEdge, EdgeProvider }

class ErdosRenyi(scale: Int, ratio: Double) extends EdgeProvider[SimpleEdge] {
  require(ratio < 1 && ratio > 0)

  val V = 1 << scale
  val range = 1 << 30
  val probability = (range * ratio).toInt

  def vertices = Iterator.from(0).take(V)

  def getEdges = for (u <- vertices; v <- vertices if Random.nextInt(range) < probability)
    yield Edge(u, v)
}

class ErdosRenyiSimplified(scale: Int, degree: Int) extends EdgeProvider[SimpleEdge] {
  val V = 1 << scale
  val E = V.toLong * degree

  var nEdge = E

  def getEdges =
    Iterator.continually { Edge(Random.nextInt(V), Random.nextInt(V)) }
      .takeWhile { _ => nEdge -= 1; nEdge >= 0 }
}