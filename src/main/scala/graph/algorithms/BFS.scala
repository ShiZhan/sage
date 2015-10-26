package graph.algorithms

/*
 * BFS:    BFS on directed graphs
 * BFS_U:  BFS on undirected graphs
 */
import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm

class BFS(root: Int) extends Algorithm[SimpleEdge, Int](0) {
  vertices(root) = 1
  gather.add(root)
  var d = 2

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges if (gather(u) && vertices.unVisited(v))) vertices.synchronized {
      vertices(v) = d; scatter.add(v)
    }

  def update() = d += 1
}

class BFS_U(root: Int) extends Algorithm[SimpleEdge, Int](0) {
  vertices(root) = 1
  gather.add(root)
  var d = 2

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) vertices.synchronized {
      if (gather(u) && vertices.unVisited(v)) { vertices(v) = d; scatter.add(v) }
      if (gather(v) && vertices.unVisited(u)) { vertices(u) = d; scatter.add(u) }
    }

  def update() = d += 1
}
