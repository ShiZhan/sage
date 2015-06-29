package algorithms.parallel

import graph.{ Edge, EdgeProvider, SimpleEdge }
import algorithms.Algorithm

case class PRValue(value: Double, sum: Double, deg: Int) {
  def addDeg = PRValue(value, sum, deg + 1)
  def initPR(implicit nVertex: Long) = PRValue(1 / nVertex, sum, deg)
  def gather(delta: Double) = PRValue(value, sum + delta, deg)
  def scatter = value / deg
  def update(implicit nVertex: Long) = PRValue(0.15d / nVertex + sum * 0.85d, 0.0d, deg)
  override def toString = "%f".format(value)
}

class PageRank(nLoop: Int)(implicit eps: Seq[EdgeProvider[SimpleEdge]])
    extends Algorithm[PRValue] {
  val initialValue = PRValue(0.0d, 0.0d, 0)

  def iterations = {
    logger.info("collect vertex degree")
    eps.par foreach { ep =>
      for (Edge(u, v) <- ep.getEdges) {
        val v0 = data.getOrElse(u, initialValue); scatter(u, v0.addDeg)
        val v1 = data.getOrElse(v, initialValue); scatter(v, v1.addDeg)
      }
    }

    logger.info("initialize PR value")
    implicit val nVertex = data.size.toLong
    for (id <- scatter) data(id) = data(id).initPR

    for (n <- (1 to nLoop)) {
      logger.info("Loop {}", n)
      eps.par foreach { ep => for (Edge(u, v) <- ep.getEdges) data(v) = data(v).gather(data(u).scatter) }
      for (id <- scatter) data(id) = data(id).update
    }
  }
}
