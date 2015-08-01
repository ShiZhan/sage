package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray
import helper.Lines.LinesWrapper

case class DirectedDegree(i: Int, o: Int) {
  def addIDeg = DirectedDegree(i + 1, o)
  def addODeg = DirectedDegree(i, o + 1)
  override def toString = s"$i $o"
}

class Degree extends Algorithm[SimpleEdge, DirectedDegree](DirectedDegree(0, 0)) {
  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) vertices.synchronized {
      vertices(u) = vertices(u).addODeg
      vertices(v) = vertices(u).addIDeg
    }

  def update() = {}
}

class Degree_U extends Algorithm[SimpleEdge, Int](0) {
  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) vertices.synchronized {
      vertices(u) = vertices(u) + 1
      vertices(v) = vertices(v) + 1
    }

  def update() = {}
}
