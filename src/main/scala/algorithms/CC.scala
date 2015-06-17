package algorithms

import graph.{ Edge, EdgeProvider }

class CC(implicit ep: EdgeProvider[Edge]) extends Algorithm[Long] {
  def iterations = {
    for (Edge(u, v) <- ep.getEdges) {
      val min = if (u < v) u else v
      if (data.getOrElse(u, Long.MaxValue) > min) scatter(u, min)
      if (data.getOrElse(v, Long.MaxValue) > min) scatter(v, min)
    }
    update

    while (!gather.isEmpty) {
      for (Edge(u, v) <- ep.getEdges) {
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