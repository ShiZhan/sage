package algorithms

import graph.{ Edge, Vertices, Shards }

class BFS(vertices: Vertices, shards: Shards) {
  def run(root: Long) = {
    var distance = 1L
    vertices.in.put(root, distance)
    vertices.data.put(root, distance)
    shards.setFlagByVertex(root)
    println(s"$root: $distance")

    val data = vertices.data
    while (!vertices.in.isEmpty) {
      val edges = shards.getFlagedEdges
      val in = vertices.in
      val out = vertices.out
      distance += 1L
      for (e <- edges) {
        val Edge(u, v) = e
        val valueU = in.get(u)
        if (valueU != 0L) {
          val valueV = data.get(v)
          if (valueV == 0L) {
            out.put(v, distance)
            shards.setFlagByVertex(v)
            println(s"$v: $distance")
          }
        }
      }
      vertices.update
    }
  }
}
