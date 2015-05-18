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
        data.put(u, true)
        data.put(v, true)
    }
    val nVertices = data.size()
    println(s"Vertices: $nVertices")
    println(s"Edges:    $nEdges")
    data.clear()
  }
}