package algorithms

import graph.{ Edge, Vertices, Shards }

class BFS(vertices: Vertices, shards: Shards) {
  def run(root: Long) = {
    var level = 1L
    vertices.out.put(root, level)
    shards.setFlagByVertex(root)
    println(s"$root: $level")
    vertices.update

    val data = vertices.data
    while (!vertices.in.isEmpty) {
      val edges = shards.getFlagedEdges
      val in = vertices.in
      val out = vertices.out
      level += 1L
      for (e <- edges) {
        val Edge(u, v) = e
        val valueU = in.get(u)
        if (valueU != 0L) {
          val valueV = data.get(v)
          if (valueV == 0L) {
            out.put(v, level)
            shards.setFlagByVertex(v)
            println(s"$v: $level")
          }
        }
      }
      vertices.update
    }
  }
}
