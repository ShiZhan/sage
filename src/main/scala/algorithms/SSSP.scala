/*
 * SSSP: Current working on directed simple graphs, thus it's the same with BFS
 * for weighted edges, simply replace the distance '+1' with accumulated path length.
 */
package algorithms

class SSSP(prefix: String, nShard: Int, root: Long)
  extends Algorithm[Long](prefix, nShard, false, "") {
  import graph.Edge

  def iterations = {
    var distance = 1L
    vertices.out.put(root, distance)
    shards.setFlagByVertex(root)
    vertices.update

    val data = vertices.data
    while (!vertices.in.isEmpty) {
      val edges = shards.getFlagedEdges
      val in = vertices.in
      val out = vertices.out

      distance += 1L
      for (Edge(u, v) <- edges if in.containsKey(u) && !data.containsKey(v)) {
        out.put(v, distance)
        shards.setFlagByVertex(v)
      }
      vertices.update
    }
    Some(vertices.result)
  }
}
