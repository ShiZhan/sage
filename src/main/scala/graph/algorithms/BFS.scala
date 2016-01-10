package graph.algorithms

/*
 * BFS:    BFS on directed graphs
 * BFS_U:  BFS on undirected graphs
 */
import graph.{ Edge, SimpleEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray

class BFS(root: Long) extends Algorithm[SimpleEdge, Long] {
  val parent = GrowingArray[Long](Long.MaxValue)
  parent(root) = -1
  gather.add(root)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges if (gather(u) && parent.unVisited(v))) parent.synchronized {
      parent(v) = u; scatter.add(v)
    }

  def update() = {}
  def complete() = parent.updated
}

class BFS_U(root: Long) extends Algorithm[SimpleEdge, Long] {
  val parent = GrowingArray[Long](Long.MaxValue)
  parent(root) = -1
  gather.add(root)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) parent.synchronized {
      if (gather(u) && parent.unVisited(v)) { parent(v) = u; scatter.add(v) }
      if (gather(v) && parent.unVisited(u)) { parent(u) = v; scatter.add(u) }
    }

  def update() = {}
  def complete() = parent.updated
}

class BFS_U_V(root: Long) extends Algorithm[SimpleEdge, (Long, Int)] {
  val parent = GrowingArray[Long](Long.MaxValue)
  val level = GrowingArray[Int](Int.MaxValue)
  parent(root) = -1
  level(root) = 0
  gather.add(root)

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) parent.synchronized {
      if (gather(u) && parent.unVisited(v)) { parent(v) = u; level(v) = step + 1; scatter.add(v) }
      if (gather(v) && parent.unVisited(u)) { parent(u) = v; level(u) = step + 1; scatter.add(u) }
    }

  def update() = {}
  def complete() = parent.updated.map { case (i, p) => (i, (p, level(i))) }
}
