package algorithms.parallel

import graph.{ Edge, EdgeProvider, SimpleEdge }

class CC(implicit eps: Seq[EdgeProvider[SimpleEdge]]) extends Algorithm[Int](Int.MaxValue) {
  def iterations() = {
    for (ep <- eps.par; Edge(u, v) <- ep.getEdges) {
      val min = if (u < v) u else v
      if (vertices(u) > min) scatter(u, min)
      if (vertices(v) > min) scatter(v, min)
    }
    update

    while (!gather.isEmpty) {
      for (ep <- eps.par; Edge(u, v) <- ep.getEdges) {
        if (gather(u)) {
          val value = vertices(u)
          if (value < vertices(v)) scatter(v, value)
        }
        if (gather(v)) {
          val value = vertices(v)
          if (value < vertices(u)) scatter(u, value)
        }
      }
      update
    }
  }
}
