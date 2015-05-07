package algorithms

import graph.{ Edge, Vertices, Shards }

class CC(shards: Shards) {
  val vertices = new Vertices[Long]("")

  def checkSet(vt: vertices.VertexTable, key: Long, value: Long) =
    if (vt.containsKey(key)) {
      if (vt.get(key) > value)
        vt.put(key, value)
    } else
      vt.put(key, value)

  def run = {
    val allEdges = shards.getAllEdges
    val out0 = vertices.out
    for (Edge(u, v) <- allEdges) {
      val value = if (u < v) u else v
      checkSet(out0, u, value)
      checkSet(out0, v, value)
    }
    shards.setAllFlags
    vertices.update

    val data = vertices.data
    while (!vertices.in.isEmpty) {
      val edges = shards.getFlagedEdges
      val in = vertices.in
      val out = vertices.out
      for (Edge(u, v) <- edges) {
        if (in.containsKey(u)) {
          val value = in.get(u)
          if (value < data.get(v)) {
            out.put(v, value)
            shards.setFlagByVertex(v)
          }
        }
        if (in.containsKey(v)) {
          val value = in.get(v)
          if (value < data.get(u)) {
            out.put(u, value)
            shards.setFlagByVertex(u)
          }
        }
      }
      vertices.update
    }
    vertices.print
  }
}

class SCC(vertices: Vertices[Long], shards: Shards) {
  def run = {
  }
}
