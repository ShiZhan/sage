/*
 * BFS:   BFS on directed graphs
 * BFS_U: BFS on undirected graphs
 * BFS_P: BFS on undirected graph with parallel shards 
 */
package algorithms

class BFS(root: Long)(implicit ep: graph.EdgeProvider)
    extends Algorithm[Long](-1L) {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter(root, level)
    update

    while (gather) {
      level += 1L
      for (Edge(u, v) <- ep.getEdges if (gather(u) && data.unused(v))) {
        scatter(v, level)
      }
      update
    }
  }
}

class BFS_U(root: Long)(implicit ep: graph.EdgeProvider)
    extends Algorithm[Long](-1L) {
  import graph.Edge

  def iterations = {
    var level = 1L
    scatter(root, level)
    update

    while (gather) {
      level += 1L
      for (Edge(u, v) <- ep.getEdges) {
        if (gather(u) && data.unused(v)) scatter(v, level)
        if (gather(v) && data.unused(u)) scatter(u, level)
      }
      update
    }
  }
}