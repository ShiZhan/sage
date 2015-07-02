package algorithms

import graph.{ Edge, EdgeProvider, SimpleEdge }
import helper.IteratorOps.VisualOperations

class KCore(implicit ep: EdgeProvider[SimpleEdge]) extends Algorithm[Long] {
  def iterations() = {
    logger.info("Preparing vertex degree ...")
    ep.getEdges.foreachDo {
      case Edge(u, v) =>
        scatter(u, data.getOrElse(u, 0L) + 1)
        scatter(v, data.getOrElse(v, 0L) + 1)
    }
    update

    var core = 1L
    while (!gather.isEmpty) {
      core = gather.map { data(_) }.min
      for (Edge(u, v) <- ep.getEdges if gather(u) && gather(v)) {
        val dU = data(u)
        val dV = data(v)
        if (dU > core && dV > core) { scatter(u, dU); scatter(v, dV) }
        else if (dU > core && dV <= core) scatter(u, dU - 1)
        else if (dU <= core && dV > core) scatter(v, dV - 1)
      }
      update
    }
  }
}