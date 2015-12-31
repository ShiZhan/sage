package graph.algorithms

/*
 * BFS:    BFS on directed graphs
 * BFS_U:  BFS on undirected graphs
 */
import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm

class BFS(root: Long) extends Algorithm[SimpleEdge, Long](Long.MaxValue) {
  vertices(root) = -1
  gather.add(root)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges if (gather(u) && vertices.unVisited(v))) vertices.synchronized {
      vertices(v) = u; scatter.add(v)
    }

  def update() = {}
}

class BFS_U(root: Long) extends Algorithm[SimpleEdge, Long](Long.MaxValue) {
  vertices(root) = -1
  gather.add(root)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) vertices.synchronized {
      if (gather(u) && vertices.unVisited(v)) { vertices(v) = u; scatter.add(v) }
      if (gather(v) && vertices.unVisited(u)) { vertices(u) = v; scatter.add(u) }
    }

  def update() = {}
}
