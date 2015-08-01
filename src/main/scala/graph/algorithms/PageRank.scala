package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
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

class PageRank(nLoop: Int) extends Algorithm[SimpleEdge, PRValue](PRValue(0.0f, 0.0f, 0)) {
  implicit var nVertex = 0

  override def forward() = stepCounter += 1
  override def hasNext() = stepCounter <= nLoop

  def compute(edges: Iterator[SimpleEdge]) = if (stepCounter == 0) {
    for (Edge(u, v) <- edges) {
      vertices(u) = vertices(u).addDeg
      vertices(v) = vertices(v).addDeg
    }
  } else {
    for (Edge(u, v) <- edges) vertices.synchronized {
      vertices(v) = vertices(v).gather(vertices(u).scatter)
    }
  }

  def update() = if (stepCounter == 0) {
    logger.info("initialize PR value")
    nVertex = vertices.nUpdated
    for ((id, value) <- vertices.updated) vertices(id) = value.initPR
  } else {
    logger.info("update PR value")
    for ((id, value) <- vertices.updated) vertices(id) = value.update
  }
}
