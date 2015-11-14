package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm

class CC extends Algorithm[SimpleEdge, Long](Long.MaxValue) {
  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) vertices.synchronized {
      if (stepCounter == 0) {
        val min = u min v
        if (vertices(u) > min) { vertices(u) = min; scatter.add(u) }
        if (vertices(v) > min) { vertices(v) = min; scatter.add(v) }
      } else {
        if (gather(u)) {
          val value = vertices(u)
          if (value < vertices(v)) { vertices(v) = value; scatter.add(v) }
        }
        if (gather(v)) {
          val value = vertices(v)
          if (value < vertices(u)) { vertices(u) = value; scatter.add(u) }
        }
      }
    }

  def update() = {}
}
