/*
 * BFS:   BFS on directed graphs
 * BFS_U: BFS on undirected graphs
 * BFS_P: BFS on undirected graph with parallel shards 
 */
package algorithms

class BFS(root: Long)(implicit context: Context)
    extends Algorithm[Long](context) {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter.put(root, level)
    update

    while (!gather.isEmpty) {
      val g = gather
      val s = scatter

      level += 1L
      for (Edge(u, v) <- E.get if (g.containsKey(u) && !data.containsKey(v))) {
        s.put(v, level)
      }
      update
    }
  }
}

class BFS_U(root: Long)(implicit context: Context)
    extends Algorithm[Long](context) {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter.put(root, level)
    update

    while (!gather.isEmpty) {
      val g = gather
      val s = scatter

      level += 1L
      for (Edge(u, v) <- E.get) {
        if (g.containsKey(u) && !data.containsKey(v)) s.put(v, level)
        if (g.containsKey(v) && !data.containsKey(u)) s.put(u, level)
      }
      update
    }
  }
}