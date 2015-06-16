/*
 * BFS:   BFS on directed graphs
 * BFS_U: BFS on undirected graphs
 * BFS_P: BFS on undirected graph with parallel shards 
 */
package algorithms

class BFS(root: Long)(implicit ep: graph.EdgeProvider)
    extends Algorithm[Long] {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter(root, level)
    update

    while (!gather.isEmpty) {
      level += 1L
      for (Edge(u, v) <- ep.getEdges if (gather(u) && !data.contains(v))) {
        scatter(v, level)
      }
      update
    }
  }
}

class BFS_U(root: Long)(implicit ep: graph.EdgeProvider)
    extends Algorithm[Long] {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter(root, level)
    update

    while (!gather.isEmpty) {
      level += 1L
      for (Edge(u, v) <- ep.getEdges) {
        if (gather(u) && !data.contains(v)) scatter(v, level)
        if (gather(v) && !data.contains(u)) scatter(u, level)
      }
      update
    }
  }
}