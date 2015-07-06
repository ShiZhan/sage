package algorithms.parallel

/**
 * Single Source Shortest Path
 * SSSP:   working on directed weighted graphs
 * SSSP_U: working on undirected weighted graphs
 */
import graph.{ Edge, WeightedEdge, EdgeProvider }

class SSSP(root: Int)(implicit eps: Seq[EdgeProvider[WeightedEdge]])
    extends Algorithm[Float](Float.MaxValue) {
  def iterations() = {
    scatter(root, 0.0f)
    update
    while (!gather.isEmpty) {
      for (ep <- eps.par; Edge(u, v, w) <- ep.getEdges if gather(u)) vertices.synchronized {
        val distance = vertices(u) + w
        if (vertices(v) > distance) scatter(v, distance)
      }
      update
    }
  }
}

class SSSP_U(root: Int)(implicit eps: Seq[EdgeProvider[WeightedEdge]])
    extends Algorithm[Float](Float.MaxValue) {
  def iterations() = {
    scatter(root, 0.0f)
    update
    while (!gather.isEmpty) {
      for (ep <- eps.par; Edge(u, v, w) <- ep.getEdges) vertices.synchronized {
        if (gather(u)) {
          val distance = vertices(u) + w
          if (vertices(v) > distance) scatter(v, distance)
        }
        if (gather(v)) {
          val distance = vertices(v) + w
          if (vertices(u) > distance) scatter(u, distance)
        }
      }
      update
    }
  }
}
