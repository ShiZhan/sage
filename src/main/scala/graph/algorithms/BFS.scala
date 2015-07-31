package algorithms

/*
 * BFS:    BFS on directed graphs
 * BFS_U:  BFS on undirected graphs
 */
import graph.{ Edge, EdgeProvider, SimpleEdge }

class BFS(root: Int)(implicit ep: EdgeProvider[SimpleEdge]) extends Algorithm[Int](0) {
  def iterations() = {
    var level = 1
    scatter(root, level)
    update

    while (gather.nonEmpty) {
      level += 1
      for (Edge(u, v) <- ep.getEdges if (gather(u) && vertices.unVisited(v))) {
        scatter(v, level)
      }
      update
    }
  }
}

class BFS_U(root: Int)(implicit ep: EdgeProvider[SimpleEdge]) extends Algorithm[Int](0) {
  def iterations() = {
    var level = 1
    scatter(root, level)
    update

    while (gather.nonEmpty) {
      level += 1
      for (Edge(u, v) <- ep.getEdges) {
        if (gather(u) && vertices.unVisited(v)) scatter(v, level)
        if (gather(v) && vertices.unVisited(u)) scatter(u, level)
      }
      update
    }
  }
}
