package algorithms

class Status(prefix: String, nShard: Int)
    extends Algorithm[Boolean](prefix, nShard, false, "") {
  import graph.Edge
  import helper.Gauge.IteratorOperations

  def iterations = {
    logger.info("Counting vertex/edge total ...")
    var nEdges = 0L
    shards.getAllEdges.foreachDo {
      case Edge(u, v) =>
        nEdges += 1
        data.put(u, true)
        data.put(v, true)
    }
    val nVertices = data.size()
    logger.info(s"Vertices: $nVertices")
    logger.info(s"Edges:    $nEdges")
    data.clear()
  }
}