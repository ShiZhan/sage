package algorithms

class CC(prefix: String, nShard: Int)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import graph.Edge

  def iterations = {
    val allEdges = shards.getAllEdges
    val s0 = scatter
    for (Edge(u, v) <- allEdges) {
      val value = if (u < v) u else v
      s0.putIfAbsent(u, value)
      s0.putIfAbsent(v, value)
    }
    update

    while (!gather.isEmpty) {
      val g = gather
      val s = scatter
      for (Edge(u, v) <- shards.getAllEdges) {
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