package algorithms

import graph.{ Edge, EdgeProvider, SimpleEdge }

case class DirectedDegree(i: Int, o: Int) {
  def addIDeg = DirectedDegree(i + 1, o)
  def addODeg = DirectedDegree(i, o + 1)
  override def toString = s"$i $o"
}

class Degree(implicit ep: EdgeProvider[SimpleEdge])
    extends Algorithm[DirectedDegree](DirectedDegree(0, 0)) {
  def iterations() = {
    logger.info("Counting vertex in and out degree ...")
    for (Edge(u, v) <- ep.getEdges) {
      vertices(u) = vertices(u).addODeg
      vertices(v) = vertices(u).addIDeg
    }
  }
}

class Degree_U(implicit ep: EdgeProvider[SimpleEdge]) extends Algorithm[Int](0) {
  def iterations() = {
    logger.info("Counting vertex degree ...")
    for (Edge(u, v) <- ep.getEdges) {
      vertices(u) = vertices(u) + 1
      vertices(v) = vertices(v) + 1
    }
  }
}
