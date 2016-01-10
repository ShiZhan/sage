package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray

class CC extends Algorithm[SimpleEdge, Long] {
  val component = GrowingArray[Long](Long.MaxValue)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) component.synchronized {
      if (step == 0) {
        val min = u min v
        if (component(u) > min) { component(u) = min; scatter.add(u) }
        if (component(v) > min) { component(v) = min; scatter.add(v) }
      } else {
        if (gather(u)) {
          val value = component(u)
          if (value < component(v)) { component(v) = value; scatter.add(v) }
        }
        if (gather(v)) {
          val value = component(v)
          if (value < component(u)) { component(u) = value; scatter.add(u) }
        }
      }
    }

  def update() = {}
  def complete() = component.updated
}
