package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.ParallelEngine.Algorithm
import helper.GrowingArray
import helper.Lines.LinesWrapper

case class PRValue(value: Float, sum: Float, deg: Int) {
  def addDeg = PRValue(value, sum, deg + 1)
  def initPR(implicit nVertex: Int) = PRValue(1 / nVertex, sum, deg)
  def gather(delta: Float) = PRValue(value, sum + delta, deg)
  def scatter = value / deg
  def update(implicit nVertex: Int) = PRValue(0.15f / nVertex + sum * 0.85f, 0.0f, deg)
  override def toString = "%f".format(value)
}

class PageRank(nLoop: Int) extends Algorithm[SimpleEdge] {
  val pr = GrowingArray[PRValue](PRValue(0.0f, 0.0f, 0))
  implicit var nVertex = 0

  override def forward() = stepCounter += 1
  override def hasNext() = stepCounter <= nLoop

  def compute(edges: Iterator[SimpleEdge]) = if (stepCounter == 0) {
    for (Edge(u, v) <- edges) {
      pr(u) = pr(u).addDeg
      pr(v) = pr(v).addDeg
    }
  } else {
    for (Edge(u, v) <- edges) pr.synchronized {
      pr(v) = pr(v).gather(pr(u).scatter)
    }
  }

  def update() = if (stepCounter == 0) {
    logger.info("initialize PR value")
    nVertex = pr.nUpdated
    for ((id, value) <- pr.updated) pr(id) = value.initPR
  } else {
    logger.info("update PR value")
    for ((id, value) <- pr.updated) pr(id) = value.update
  }

  def complete() =
    pr.synchronized { pr.updated.map { case (k, v) => s"$k $v" }.toFile("pagerank.csv") }
}
