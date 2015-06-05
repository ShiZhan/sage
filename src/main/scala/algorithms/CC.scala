package algorithms

class CC(implicit context: Context)
    extends Algorithm[Long](context, Long.MaxValue) {
  import graph.Edge

  def iterations = {
    for (Edge(u, v) <- getEdges) {
      val min = if (u < v) u else v
      if (data.get(u) > min) scatter(u, min)
      if (data.get(v) > min) scatter(v, min)
    }
    update

    while (gather) {
      for (Edge(u, v) <- getEdges) {
        if (gather(u)) {
          val value = data.get(u)
          if (value < data.get(v)) scatter(v, value)
        }
        if (gather(v)) {
          val value = data.get(v)
          if (value < data.get(u)) scatter(u, value)
        }
      }
      update
    }
  }
}