package graph

/**
 * @author Zhan
 * edge list importer
 * edgeFileName:  input edge list
 * selfloop:      allow selfloop edges
 * bidirectional: generate reverse edges
 * binary:        import from binary edge list, edges in text file or console.
 */
object Importer extends helper.Logging {
  def run(edgeFileName: String, selfloop: Boolean, bidirectional: Boolean, binary: Boolean) = {
    val edgeProvider =
      if (edgeFileName.isEmpty) Edges.fromConsole
      else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    val edges0 = edgeProvider.getEdges
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    val edgeStorage = Edges.fromFile(s"imported-$edgeFileName")
    logger.debug("STORING")
    edgeStorage.putEdges(edgesB)
    logger.debug("COMPLETE")
  }
}