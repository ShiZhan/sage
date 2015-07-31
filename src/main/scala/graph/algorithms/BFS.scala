package graph.algorithms

/*
 * BFS:    BFS on directed graphs
 * BFS_U:  BFS on undirected graphs
 */
import graph.{ Edge, SimpleEdge }
import graph.ParallelEngine.Algorithm
import helper.GrowingArray
import helper.Lines.LinesWrapper

class BFS(root: Int) extends Algorithm[SimpleEdge] {
  val distance = GrowingArray[Int](0)
  distance(root) = 1
  gather.add(root)
  var d = 2

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges if (gather(u) && distance.unVisited(v))) distance.synchronized {
      distance(v) = d; scatter.add(v)
    }

  def update() = d += 1

  def complete() = distance.updated
}

class BFS_U(root: Int) extends Algorithm[SimpleEdge] {
  val distance = GrowingArray[Int](0)
  distance(root) = 1
  gather.add(root)
  var d = 2

  def compute(edges: Iterator[SimpleEdge]) =
    for (Edge(u, v) <- edges) distance.synchronized {
      if (gather(u) && distance.unVisited(v)) { distance(v) = d; scatter.add(v) }
      if (gather(v) && distance.unVisited(u)) { distance(u) = d; scatter.add(u) }
    }

  def update() = d += 1

  def complete() = distance.updated
}
