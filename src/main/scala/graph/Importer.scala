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
object Importer {
  def importSimpleEdges(edgeFileName: String, selfloop: Boolean,
    bidirectional: Boolean, binary: Boolean) = {
    val edgeProvider =
      if (edgeFileName.isEmpty) Edges.fromConsole
      else if (binary) Edges.fromFile(edgeFileName) else Edges.fromText(edgeFileName)
    val edgeStorage = Edges.fromFile(s"$edgeFileName-imported.bin")
    val edges0 = edgeProvider.getEdges
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    edgeStorage.putEdges(edgesB)
  }

  def importWeightedEdges(edgeFileName: String, selfloop: Boolean,
    bidirectional: Boolean, binary: Boolean) = {
    val edgeProvider =
      if (edgeFileName.isEmpty) WEdges.fromConsole
      else if (binary) WEdges.fromFile(edgeFileName) else WEdges.fromText(edgeFileName)
    val edgeStorage = WEdges.fromFile(s"$edgeFileName-imported.bin")
    val edges0 = edgeProvider.getEdges
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    edgeStorage.putEdges(edgesB)
  }

  def run(edgeFileName: String, selfloop: Boolean, bidirectional: Boolean, binary: Boolean, weight: Boolean) =
    if (weight)
      importWeightedEdges(edgeFileName, selfloop, bidirectional, binary)
    else
      importSimpleEdges(edgeFileName, selfloop, bidirectional, binary)
}