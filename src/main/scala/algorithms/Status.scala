package algorithms

class Status(implicit context: Context)
    extends Algorithm[Boolean](context) {
  import graph.Edge
  import helper.IteratorOps.VisualOperations

  def iterations = {
    logger.info("Counting vertex/edge total ...")
    var nEdges = 0L
    getEdges.foreachDo {
      case Edge(u, v) =>
        nEdges += 1
        data.put(u, true)
        data.put(v, true)
    }
    val nVertices = data.size
    logger.info(s"Vertices: $nVertices")
    logger.info(s"Edges:    $nEdges")
    data.clear()
  }
}