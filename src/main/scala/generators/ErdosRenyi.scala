package generators

import scala.util.Random
import graph.{ Edge, EdgeProvider }

class ErdosRenyi(scale: Int, ratio: Double) extends EdgeProvider[Edge] {
  require(ratio < 1 && ratio > 0)

  val total = 1L << scale
  val range = 1 << 20
  val probability = (range * ratio).toInt

  def vertices = {
    var vID = -1L
    Iterator.continually { vID += 1; vID }.takeWhile(_ < total)
  }

  def getEdges = for (u <- vertices; v <- vertices if Random.nextInt(range) < probability)
    yield Edge(u, v)
}

class ErdosRenyiSimplified(scale: Int, degree: Int) extends EdgeProvider[Edge] {
  val V = 1L << scale
  val E = V * degree
  val M = V - 1

  var nEdge = E

  def getEdges =
    Iterator.continually { Edge(Random.nextLong & M, Random.nextLong & M) }
      .takeWhile { _ => nEdge -= 1; nEdge >= 0 }
}