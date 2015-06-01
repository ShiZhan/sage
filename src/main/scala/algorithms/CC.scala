package algorithms

class CC(implicit context: Context)
    extends Algorithm[Long](context) {
  import graph.Edge

  def iterations = {
    val s0 = scatter
    for (Edge(u, v) <- getEdges) {
      val min = if (u < v) u else v
      if (!s0.containsKey(u)) s0.put(u, min) else if (s0.get(u) > min) s0.put(u, min)
      if (!s0.containsKey(v)) s0.put(v, min) else if (s0.get(v) > min) s0.put(v, min)
    }
    update

    while (!gather.isEmpty) {
      val g = gather
      val s = scatter
      for (Edge(u, v) <- getEdges) {
        if (g.containsKey(u)) {
          val value = g.get(u)
          if (value < data.get(v)) s.put(v, value)
        }
        if (g.containsKey(v)) {
          val value = g.get(v)
          if (value < data.get(u)) s.put(u, value)
        }
      }
      update
    }
  }
}