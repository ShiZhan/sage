package algorithms.parallel

import graph.{ Edge, EdgeProvider, SimpleEdge }

case class PRValue(value: Float, sum: Float, deg: Int) {
  def addDeg = PRValue(value, sum, deg + 1)
  def initPR(implicit nVertex: Int) = PRValue(1 / nVertex, sum, deg)
  def gather(delta: Float) = PRValue(value, sum + delta, deg)
  def scatter = value / deg
  def update(implicit nVertex: Int) = PRValue(0.15f / nVertex + sum * 0.85f, 0.0f, deg)
  override def toString = "%f".format(value)
}

class PageRank(nLoop: Int)(implicit eps: Seq[EdgeProvider[SimpleEdge]])
    extends Algorithm[PRValue](PRValue(0.0f, 0.0f, 0)) {
  def iterations() = {
    logger.info("collect vertex degree")
    for (ep <- eps.par; Edge(u, v) <- ep.getEdges) {
      val v0 = vertices(u); scatter(u, v0.addDeg)
      val v1 = vertices(v); scatter(v, v1.addDeg)
    }

    logger.info("initialize PR value")
    implicit val nVertex = vertices.nUpdated
    for (id <- scatter) vertices(id) = vertices(id).initPR

    for (n <- (1 to nLoop)) {
      logger.info("Loop {}", n)
      for (ep <- eps.par; Edge(u, v) <- ep.getEdges) vertices.synchronized {
        vertices(v) = vertices(v).gather(vertices(u).scatter)
      }
      for (id <- scatter) vertices(id) = vertices(id).update
    }
  }
}
