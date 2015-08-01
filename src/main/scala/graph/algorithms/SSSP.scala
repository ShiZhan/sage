package graph.algorithms

/**
 * Single Source Shortest Path
 * SSSP:   working on directed weighted graphs
 * SSSP_U: working on undirected weighted graphs
 */
import graph.{ Edge, WeightedEdge }
import graph.Parallel.Algorithm
import helper.GrowingArray
import helper.Lines.LinesWrapper

class SSSP(root: Int) extends Algorithm[WeightedEdge, Float](Float.MaxValue) {
  vertices(root) = 0.0f
  gather.add(root)

  def compute(edges: Iterator[WeightedEdge]) =
    for (Edge(u, v, w) <- edges if (gather(u)))
      vertices.synchronized {
        val d = vertices(u) + w
        if (vertices(v) > d) {
          vertices(v) = d
          scatter.add(v)
        }
      }

  def update() = {}
}

class SSSP_U(root: Int) extends Algorithm[WeightedEdge, Float](Float.MaxValue) {
  vertices(root) = 0.0f
  gather.add(root)

  def compute(edges: Iterator[WeightedEdge]) =
    for (Edge(u, v, w) <- edges) vertices.synchronized {
      if (gather(u)) {
        val d = vertices(u) + w
        if (vertices(v) > d) {
          vertices(v) = d
          scatter.add(v)
        }
      }
      if (gather(v)) {
        val d = vertices(v) + w
        if (vertices(u) > d) {
          vertices(u) = d
          scatter.add(u)
        }
      }
    }

  def update() = {}
}
