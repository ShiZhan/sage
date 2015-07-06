package algorithms.parallel

import graph.{ Edge, EdgeProvider, SimpleEdge }

class CC(implicit eps: Seq[EdgeProvider[SimpleEdge]]) extends Algorithm[Int](Int.MaxValue) {
  def iterations() = {
    for (ep <- eps.par; Edge(u, v) <- ep.getEdges) {
      val min = if (u < v) u else v
      if (data(u) > min) scatter(u, min)
      if (data(v) > min) scatter(v, min)
    }
    update

    while (!gather.isEmpty) {
      for (ep <- eps.par; Edge(u, v) <- ep.getEdges) {
        if (gather(u)) {
          val value = data(u)
          if (value < data(v)) scatter(v, value)
        }
        if (gather(v)) {
          val value = data(v)
          if (value < data(u)) scatter(u, value)
        }
      }
      update
    }
  }
}
