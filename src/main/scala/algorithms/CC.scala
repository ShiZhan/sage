package algorithms

class CC(implicit context: Context)
    extends Algorithm[Long](context) {
  import graph.Edge

  def iterations = {
    for (Edge(u, v) <- getEdges) {
      val min = if (u < v) u else v
      if (data.getOrElse(u, Long.MaxValue) > min) scatter(u, min)
      if (data.getOrElse(v, Long.MaxValue) > min) scatter(v, min)
    }
    update

    while (gather) {
      for (Edge(u, v) <- getEdges) {
        if (gather(u)) {
          val value = data.getOrElse(u, Long.MaxValue)
          if (value < data.getOrElse(v, Long.MaxValue)) scatter(v, value)
        }
        if (gather(v)) {
          val value = data.getOrElse(v, Long.MaxValue)
          if (value < data.getOrElse(u, Long.MaxValue)) scatter(u, value)
        }
      }
      update
    }
  }
}