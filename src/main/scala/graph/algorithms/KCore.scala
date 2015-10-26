package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm

class KCore extends Algorithm[SimpleEdge, Int](0) {
  var c = 1

  def compute(edges: Iterator[SimpleEdge]) = for (Edge(u, v) <- edges) vertices.synchronized {
    if (stepCounter == 0) {
      vertices(u) = vertices(u) + 1; scatter.add(u)
      vertices(v) = vertices(v) + 1; scatter.add(v)
    } else if (gather(u) && gather(v)) {
      val dU = vertices(u)
      val dV = vertices(v)
      if (dU > c && dV > c) { scatter.add(u); scatter.add(v) }
      else if (dU > c && dV <= c) { vertices(u) = dU - 1; scatter.add(u) }
      else if (dU <= c && dV > c) { vertices(v) = dV - 1; scatter.add(v) }
    }
  }

  def update() = if (scatter.nonEmpty) c = scatter.view.map { vertices(_) }.min
}
