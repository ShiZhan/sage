package algorithms

import graph.{ Edge, EdgeProvider, SimpleEdge }

class KCore(implicit ep: EdgeProvider[SimpleEdge]) extends Algorithm[Int](0) {
  def iterations() = {
    logger.info("Preparing vertex degree ...")
    for (Edge(u, v) <- ep.getEdges) {
      scatter(u, vertices(u) + 1)
      scatter(v, vertices(v) + 1)
    }
    update

    var core = 1L
    while (gather.nonEmpty) {
      core = gather.map { vertices(_) }.min
      for (Edge(u, v) <- ep.getEdges if gather(u) && gather(v)) {
        val dU = vertices(u)
        val dV = vertices(v)
        if (dU > core && dV > core) { scatter(u, dU); scatter(v, dV) }
        else if (dU > core && dV <= core) scatter(u, dU - 1)
        else if (dU <= core && dV > core) scatter(v, dV - 1)
      }
      update
    }
  }
}