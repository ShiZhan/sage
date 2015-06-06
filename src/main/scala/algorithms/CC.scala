package algorithms

class CC(implicit context: Context)
    extends Algorithm[Long](context, Long.MaxValue) {
  import graph.Edge

  def iterations = {
    for (Edge(u, v) <- getEdges) {
      val min = if (u < v) u else v
      if (data(u) > min) scatter(u, min)
      if (data(v) > min) scatter(v, min)
    }
    update

    while (gather) {
      for (Edge(u, v) <- getEdges) {
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