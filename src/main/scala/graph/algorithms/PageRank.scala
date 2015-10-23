package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray

class PageRank(nLoop: Int) extends Algorithm[SimpleEdge, Double](0.0d) {
  val deg = GrowingArray[Int](0)
  val sum = GrowingArray[Double](0.0d)
  val flg = flags(0)
  lazy val nVertex = flg.size

  override def forward() = stepCounter += 1
  override def hasNext() = stepCounter <= nLoop

  def compute(edges: Iterator[SimpleEdge]) = if (stepCounter == 0) {
    for (Edge(u, v) <- edges) {
      deg(u) = deg(u) + 1
      flg.add(u)
      flg.add(v)
    }
  } else {
    for (Edge(u, v) <- edges) vertices.synchronized {
      sum(v) = sum(v) + vertices(u) / deg(u)
    }
  }

  def update() = if (stepCounter > 0) {
    logger.info("update PR value")
    for (id <- flg) {
      vertices(id) = 0.15d / nVertex + sum(id) * 0.85d
      sum(id) = 0.0d
    }
  }
}
