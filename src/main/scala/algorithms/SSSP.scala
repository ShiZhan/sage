/*
 * SSSP: Current working on directed simple graphs, thus it's the same with BFS
 * for weighted edges, simply replace the distance '+1' with accumulated path length.
 */
package algorithms

class SSSP(root: Long)(implicit ep: graph.EdgeProvider)
    extends Algorithm[Long] {
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