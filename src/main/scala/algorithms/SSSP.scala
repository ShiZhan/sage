package algorithms

/**
 * Single Source Shortest Path
 * SSSP:    working on directed simple graphs, thus it's the same with BFS
 * SSSP_W:  working on directed weighted graphs
 * SSSP_UW: working on undirected weighted graphs
 */
import graph.{ Edge, WEdge, EdgeProvider }

class SSSP(root: Long)(implicit ep: EdgeProvider[Edge]) extends Algorithm[Long] {
  def iterations = {
    var distance = 1L
    scatter(root, distance)
    update
    while (!gather.isEmpty) {
      distance += 1L
      for (Edge(u, v) <- ep.getEdges if gather(u) && !data.contains(v))
        scatter(v, distance)
      update
    }
  }
}

class SSSP_W(root: Long)(implicit ep: EdgeProvider[WEdge]) extends Algorithm[Float] {
  def iterations = {
    scatter(root, 0.0f)
    update
    while (!gather.isEmpty) {
      for (WEdge(u, v, w) <- ep.getEdges if gather(u)) {
        val distance = data(u) + w
        val target = data.getOrElse(v, Float.MaxValue)
        if (target > distance) scatter(v, distance)
      }
      update
    }
  }
}

class SSSP_WU(root: Long)(implicit ep: EdgeProvider[WEdge]) extends Algorithm[Float] {
  def iterations = {
    scatter(root, 0.0f)
    update
    while (!gather.isEmpty) {
      for (WEdge(u, v, w) <- ep.getEdges) {
        if (gather(u)) {
          val distance = data(u) + w
          val target = data.getOrElse(v, Float.MaxValue)
          if (target > distance) scatter(v, distance)
        }
        if (gather(v)) {
          val distance = data(v) + w
          val target = data.getOrElse(u, Float.MaxValue)
          if (target > distance) scatter(u, distance)
        }
      }
      update
    }
  }
}