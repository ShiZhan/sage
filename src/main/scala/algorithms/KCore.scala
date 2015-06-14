package algorithms

class KCore(implicit ep: graph.EdgeProvider)
    extends Algorithm[Long](0) {
  import graph.Edge
  import helper.IteratorOps.VisualOperations

  def iterations = {
    logger.info("Preparing vertex degree ...")
    ep.getEdges.foreachDo {
      case Edge(u, v) =>
        Seq(u, v).foreach { k => val d = data(k); scatter(k, d + 1) }
    }
    var core = scatter.map(data(_)).min
    update

    while (gather) {
      for (Edge(u, v) <- ep.getEdges) {
        val dU = data(u)
        val dV = data(v)
        if (dU > core && dV > core) { scatter(u, dU); scatter(v, dV) }
        else if (dU > core && dV <= core) scatter(u, core - 1)
        else if (dU <= core && dV > core) scatter(v, core - 1)
      }
      core = scatter.map(data(_)).min
      update
    }
  }
}