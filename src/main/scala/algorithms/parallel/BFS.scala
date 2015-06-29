package algorithms.parallel

/*
 * BFS:    BFS on directed graphs and parallel edge lists
 * BFS_U:  BFS on undirected graphs and parallel edge lists
 */
import graph.{ Edge, EdgeProvider, SimpleEdge }
import algorithms.Algorithm

class BFS(root: Long)(implicit eps: Seq[EdgeProvider[SimpleEdge]]) extends Algorithm[Long] {
  def iterations = {
    var level = 1L
    scatter(root, level)
    update

    while (!gather.isEmpty) {
      level += 1L
      eps.par foreach { ep =>
        for (Edge(u, v) <- ep.getEdges if (gather(u) && !data.contains(v))) {
          scatter(v, level)
        }
      }
      update
    }
  }
}

class BFS_U(root: Long)(implicit eps: Seq[EdgeProvider[SimpleEdge]]) extends Algorithm[Long] {
  def iterations = {
    var level = 1L
    scatter(root, level)
    update

    while (!gather.isEmpty) {
      level += 1L
      eps.par foreach { ep =>
        for (Edge(u, v) <- ep.getEdges) {
          if (gather(u) && !data.contains(v)) scatter(v, level)
          if (gather(v) && !data.contains(u)) scatter(u, level)
        }
      }
      update
    }
  }
}
