package algorithms

import graph.{ Edge, Vertices, Shards }

class BFS(vertices: Vertices, shards: Shards) {
  def run(root: Long) = {
    var distance = 1L
    vertices.input.put(root, distance)
    shards.setFlagByVertex(root)
    println(s"$root: $distance")

    while (!vertices.input.isEmpty) {
      val edges = shards.getFlagedShards.flatMap(_.getEdges)
      val input = vertices.input
      val output = vertices.output
      distance += 1L
      for (e <- edges) {
        val Edge(u, v) = e
        val valueU = input.get(u)
        if (valueU != 0L) {
          val valueV = input.get(v)
          if (valueV == 0L) {
            output.put(v, distance)
            shards.setFlagByVertex(v)
            println(s"$v: $distance")
          }
        }
      }
      vertices.next
    }
  }
}
