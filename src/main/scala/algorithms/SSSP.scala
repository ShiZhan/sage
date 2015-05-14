/*
 * SSSP: Current working on directed simple graphs, thus it's the same with BFS
 * for weighted edges, simply replace the distance '+1' with accumulated path length.
 */
package algorithms

import graph.{ Edge, Vertices, Shards }

class SSSP(shards: Shards) {
  val vertices = Vertices[Long]

  def run(root: Long) = {
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
    vertices.print
  }
}
