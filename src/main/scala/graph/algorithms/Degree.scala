package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.ParallelEngine.Algorithm
import helper.GrowingArray
import helper.Lines.LinesWrapper

case class DirectedDegree(i: Int, o: Int) {
  def addIDeg = DirectedDegree(i + 1, o)
  def addODeg = DirectedDegree(i, o + 1)
  override def toString = s"$i $o"
}

class Degree extends Algorithm[SimpleEdge] {
  val degree = GrowingArray[DirectedDegree](DirectedDegree(0, 0))

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) degree.synchronized {
      degree(u) = degree(u).addODeg
      degree(v) = degree(u).addIDeg
    }

  def update() = {}

  def complete() = degree.updated
}

class Degree_U extends Algorithm[SimpleEdge] {
  val degree = GrowingArray[Int](0)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) degree.synchronized {
      degree(u) = degree(u) + 1
      degree(v) = degree(v) + 1
    }

  def update() = {}

  def complete() = degree.updated
}
