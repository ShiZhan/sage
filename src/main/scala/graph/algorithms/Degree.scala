package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray

case class DirectedDegree(i: Int, o: Int) {
  override def toString = s"$i $o"
}

class Degree extends Algorithm[SimpleEdge, DirectedDegree](DirectedDegree(0, 0)) {
  val i = GrowingArray[Int](0)
  val o = GrowingArray[Int](0)
  val f = gather

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) {
      o(u) = o(u) + 1; f.add(u)
      i(v) = i(v) + 1; f.add(v)
    }

  def update() = for (id <- f) vertices(id) = DirectedDegree(i(id), o(id))
}

class Degree_U extends Algorithm[SimpleEdge, Int](0) {
  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) vertices.synchronized {
      vertices(u) = vertices(u) + 1
      vertices(v) = vertices(v) + 1
    }

  def update() = {}
}
