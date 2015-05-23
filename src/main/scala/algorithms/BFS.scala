/*
 * BFS:   BFS on directed graphs
 * BFS_U: BFS on undirected graphs
 * BFS_P: BFS on undirected graph with parallel shards 
 */
package algorithms

class BFS(root: Long)(implicit context: Context)
    extends DirectionalAlgorithm[Long](context) {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter.put(root, level)
    shards.setFlagByVertex(root)
    update

    while (!gather.isEmpty) {
      val edges = shards.getFlagedEdges
      val g = gather
      val s = scatter

      level += 1L
      for (Edge(u, v) <- edges if (g.containsKey(u) && !data.containsKey(v))) {
        s.put(v, level)
        shards.setFlagByVertex(v)
      }
      update
    }
  }
}

class BFS_U(root: Long)(implicit context: Context)
    extends SimpleAlgorithm[Long](context) {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter.put(root, level)
    update

    while (!gather.isEmpty) {
      val edges = shards.getAllEdges
      val g = gather
      val s = scatter

      level += 1L
      for (Edge(u, v) <- edges) {
        if (g.containsKey(u) && !data.containsKey(v)) s.put(v, level)
        if (g.containsKey(v) && !data.containsKey(u)) s.put(u, level)
      }
      update
    }
  }
}

class BFS_R(root: Long)(implicit context: Context)
    extends BidirectionalAlgorithm[Long](context) {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter.put(root, level)
    shards.setFlagByVertex(root)
    update

    while (!gather.isEmpty) {
      val edges = shards.getFlagedEdges
      val g = gather
      val s = scatter

      level += 1L
      for (Edge(u, v) <- edges if (g.containsKey(u) && !data.containsKey(v))) {
        s.put(v, level)
        shards.setFlagByVertex(v)
      }
      update
    }
  }
}