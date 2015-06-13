package algorithms

class KCore(implicit context: Context)
    extends Algorithm[Long](context, 0) {
  import graph.Edge
  import helper.IteratorOps.VisualOperations

  def iterations = {
    logger.info("Preparing vertex degree ...")
    getEdges.foreachDo { case Edge(u, v) => Seq(u, v).foreach { k => val d = data(k); scatter(k, d + 1) } }
    update
    var core = scatter.map(data(_)).min

    while (gather) {
      for (Edge(u, v) <- getEdges) {
        val dU = data(u)
        val dV = data(v)
        if (dU > core && dV > core) { scatter(u, dU); scatter(v, dV) }
        else if (dU > core && dV <= core) scatter(u, core - 1)
        else if (dU <= core && dV > core) scatter(v, core - 1)
      }
      update
    }
  }
}