package algorithms.parallel

import graph.{ Edge, EdgeProvider, SimpleEdge }

class KCore(implicit eps: Seq[EdgeProvider[SimpleEdge]]) extends Algorithm[Int](0) {
  def iterations() = {
    logger.info("Preparing vertex degree ...")
    for (ep <- eps.par; Edge(u, v) <- ep.getEdges) data.synchronized {
      scatter(u, data(u) + 1)
      scatter(v, data(v) + 1)
    }
    update

    var core = 1
    while (!gather.isEmpty) {
      core = gather.map { data(_) }.min
      for (ep <- eps.par; Edge(u, v) <- ep.getEdges if gather(u) && gather(v)) data.synchronized {
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
