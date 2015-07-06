package algorithms.parallel

import graph.{ Edge, EdgeProvider, SimpleEdge }

case class DirectedDegree(i: Int, o: Int) {
  def addIDeg = DirectedDegree(i + 1, o)
  def addODeg = DirectedDegree(i, o + 1)
  override def toString = s"$i $o"
}

class Degree(implicit eps: Seq[EdgeProvider[SimpleEdge]])
    extends Algorithm[DirectedDegree](DirectedDegree(0, 0)) {
  def iterations() = {
    logger.info("Counting vertex in and out degree ...")
    for (ep <- eps.par; Edge(u, v) <- ep.getEdges) vertices.synchronized {
      vertices(u) = vertices(u).addODeg
      vertices(v) = vertices(u).addIDeg
    }
  }
}

class Degree_U(implicit eps: Seq[EdgeProvider[SimpleEdge]]) extends Algorithm[Int](0) {
  def iterations() = {
    logger.info("Counting vertex degree ...")
    for (ep <- eps.par; Edge(u, v) <- ep.getEdges) vertices.synchronized {
      vertices(u) = vertices(u) + 1
      vertices(v) = vertices(v) + 1
    }
  }
}
