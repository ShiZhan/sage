package algorithms

/*
 * BFS:    BFS on directed graphs
 * BFS_U:  BFS on undirected graphs
 */
import graph.{ Edge, EdgeProvider, SimpleEdge }

class BFS(root: Int)(implicit ep: EdgeProvider[SimpleEdge]) extends Algorithm[Int] {
  def iterations() = {
    var level = 1
    scatter(root, level)
    update

    while (!gather.isEmpty) {
      level += 1
      for (Edge(u, v) <- ep.getEdges if (gather(u) && !data.contains(v))) {
        scatter(v, level)
      }
      update
    }
  }
}

class BFS_U(root: Int)(implicit ep: EdgeProvider[SimpleEdge]) extends Algorithm[Int] {
  def iterations() = {
    var level = 1
    scatter(root, level)
    update

    while (!gather.isEmpty) {
      level += 1
      for (Edge(u, v) <- ep.getEdges) {
        if (gather(u) && !data.contains(v)) scatter(v, level)
        if (gather(v) && !data.contains(u)) scatter(u, level)
      }
      update
    }
  }
}