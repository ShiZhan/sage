package graph

/**
 * @author Zhan
 * edge list importer
 * edgeFileName:  input edge list
 * selfloop:      allow selfloop edges
 * bidirectional: generate reverse edges
 * binary:        import from binary edge list, edges in text file or console.
 * weight:        import as weighted edges, generate random weight if required.
 */
object Importer extends helper.Logging {
  def run(edgeFileName: String, selfloop: Boolean, bidirectional: Boolean, binary: Boolean, weight: Boolean) = {
    lazy val edgeProvider =
      if (edgeFileName.isEmpty) Edges.fromConsole
      else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    lazy val wEdgeProvider =
      if (edgeFileName.isEmpty) WEdges.fromConsole
      else if (binary) WEdges.fromFile(edgeFileName) else WEdges.fromText(edgeFileName)
    lazy val edgeStorage = Edges.fromFile(s"imported-$edgeFileName")
    lazy val wEdgeStorage = WEdges.fromFile(s"weighted-$edgeFileName")

    logger.debug("STORING")
    if (weight) {
      val edges0 = wEdgeProvider.getEdges
      val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
      val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
      wEdgeStorage.putEdges(edgesB)
    } else {
      val edges0 = edgeProvider.getEdges
      val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
      val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
      edgeStorage.putEdges(edgesB)
    }
    logger.debug("COMPLETE")
  }
}
