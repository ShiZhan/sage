package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray

class PageRank(nLoop: Int) extends Algorithm[SimpleEdge, Float](0.0f) {
  val deg = GrowingArray[Int](0)
  val sum = GrowingArray[Float](0.0f)
  val flg = gather
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
    for (Edge(u, v) <- edges) sum.synchronized {
      sum(v) = sum(v) + vertices(u) / deg(u)
    }
  }

  def update() = if (stepCounter == 0)
    logger.info("degree distribution collected")
  else {
    logger.info("iteration {} completed", stepCounter)
    for (id <- flg) {
      vertices(id) = 0.15f / nVertex + sum(id) * 0.85f
      sum(id) = 0.0f
    }
  }
}
