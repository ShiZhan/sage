package algorithms.parallel

import graph.{ Edge, EdgeProvider, SimpleEdge }

class KCore(implicit eps: Seq[EdgeProvider[SimpleEdge]]) extends Algorithm[Int](0) {
  def iterations() = {
    logger.info("Preparing vertex degree ...")
    for (ep <- eps.par; Edge(u, v) <- ep.getEdges) vertices.synchronized {
      scatter(u, vertices(u) + 1)
      scatter(v, vertices(v) + 1)
    }
    update

    var core = 1
    while (gather.nonEmpty) {
      core = gather.view.map { vertices(_) }.min
      for (ep <- eps.par; Edge(u, v) <- ep.getEdges if gather(u) && gather(v)) vertices.synchronized {
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
