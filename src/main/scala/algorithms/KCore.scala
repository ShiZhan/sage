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
    update

    var core = 1L
    while (!gather.isEmpty) {
      core = gather.map { data(_) }.min
      for (Edge(u, v) <- ep.getEdges if gather(u) && gather(v)) {
        val dU = data(u)
        val dV = data(v)
        if (dU > core && dV > core) { scatter(u, dU); scatter(v, dV) }
        else if (dU > core && dV <= core) scatter(u, dU - 1)
        else if (dU <= core && dV > core) scatter(v, dV - 1)
      }
      update
    }
  }
}