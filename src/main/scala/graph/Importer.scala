package graph

/**
 * @author Zhan
 * edge list importer
 * edgeFileName:  input edge list
 * selfloop:      allow selfloop edges
 * bidirectional: generate reverse edges
 * binary:        import from binary edge list, edges in text file or console.
 * weight:        import simple edges and generate random weight.
 */
object Importer extends helper.Logging {
  import EdgeUtils.WEdgeConverter

  def run(edgeFileName: String, selfloop: Boolean, bidirectional: Boolean, binary: Boolean, weight: Boolean) = {
    val edgeProvider =
      if (edgeFileName.isEmpty) Edges.fromConsole
      else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    val edges0 = edgeProvider.getEdges
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    lazy val edgeStorage = Edges.fromFile(s"imported-$edgeFileName")
    lazy val wEdgeStorage = WEdges.fromFile(s"weighted-$edgeFileName")

    logger.debug("STORING")
    if (weight) wEdgeStorage.putEdges(edgesB.toWEdges) else edgeStorage.putEdges(edgesB)
    logger.debug("COMPLETE")
  }
}