/*
 * SSSP: Current working on directed simple graphs, thus it's the same with BFS
 * for weighted edges, simply replace the distance '+1' with accumulated path length.
 */
package algorithms

class SSSP(root: Long)(implicit context: Context)
    extends SimpleAlgorithm[Long](context) {
  import graph.Edge

  def iterations = {
    var distance = 1L
    scatter.put(root, distance)
    shards.setFlagByVertex(root)
    update

    while (!gather.isEmpty) {
      val g = gather
      val s = scatter
      distance += 1L
      for (Edge(u, v) <- shards.getFlagedEdges if g.containsKey(u) && !data.containsKey(v)) {
        s.put(v, distance)
        shards.setFlagByVertex(v)
      }
      update
    }
  }
}