package algorithms

/**
 * Single Source Shortest Path
 * SSSP:   working on directed simple graphs, thus it's the same with BFS
 * SSSP_W: working on directed weighted graphs
 */
class SSSP(root: Long)(implicit ep: graph.EdgeProvider) extends Algorithm[Long] {
  import graph.Edge

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

class SSSP_W(root: Long)(implicit ep: graph.EdgeProvider) extends Algorithm[Long] {
  def iterations = {
  }
}