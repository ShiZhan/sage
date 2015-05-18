package algorithms

class Status(prefix: String, nShard: Int)
    extends Algorithm[Boolean](prefix, nShard, false, "") {
  import graph.Edge
  import helper.Gauge.IteratorOperations

  def iterations = {
    var nEdges = 0L
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        nEdges += 1
        vertices.data.put(u, true)
        vertices.data.put(v, true)
    }
    val nVertices = vertices.data.size()
    println(s"Vertices: $nVertices")
    println(s"Edges:    $nEdges")
    None
  }
}