package graph.algorithms

import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray

class KCore extends Algorithm[SimpleEdge, Int] {
  val core = GrowingArray[Int](0)
  var c = 1

  def compute(edges: Iterator[SimpleEdge]) = for (Edge(u, v) <- edges) core.synchronized {
    if (step == 0) {
      core(u) = core(u) + 1; scatter.add(u)
      core(v) = core(v) + 1; scatter.add(v)
    } else if (gather(u) && gather(v)) {
      val dU = core(u)
      val dV = core(v)
      if (dU > c && dV > c) { scatter.add(u); scatter.add(v) }
      else if (dU > c && dV <= c) { core(u) = dU - 1; scatter.add(u) }
      else if (dU <= c && dV > c) { core(v) = dV - 1; scatter.add(v) }
    }
  }

  def update() = if (scatter.nonEmpty) c = scatter.iterator.map { core(_) }.min
  def complete() = core.updated
}
