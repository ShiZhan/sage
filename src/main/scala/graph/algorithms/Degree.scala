package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.{ GrowingArray, GrowingBitSet }

class Degree extends Algorithm[SimpleEdge, (Int, Int)] {
  val i = GrowingArray[Int](0)
  val o = GrowingArray[Int](0)
  val flag = new GrowingBitSet

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) {
      o(u) = o(u) + 1; flag.add(u)
      i(v) = i(v) + 1; flag.add(v)
    }

  def update() = {}
  def complete() = flag.iterator.map { id => (id, (i(id), o(id))) }
}

class Degree_U extends Algorithm[SimpleEdge, Int] {
  val d = GrowingArray[Int](0)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) d.synchronized {
      d(u) = d(u) + 1
      d(v) = d(v) + 1
    }

  def update() = {}
  def complete() = d.updated
}
