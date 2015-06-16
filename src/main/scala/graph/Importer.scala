package graph

/**
 * @author Zhan
 * edge list importer
 * edgeFile:      input edge list
 * selfloop:      allow selfloop edges
 * bidirectional: generate reverse edges
 * binary:        import from binary edge list, edges in text file or console.
 */
object Importer extends helper.Logging {
  import Edges._

  def run(edgeFile: String, selfloop: Boolean, bidirectional: Boolean, binary: Boolean) = {
    val ofn = if (edgeFile.isEmpty) "graph.bin" else edgeFile + ".bin"
    val edges0 = if (binary) EdgeFile(edgeFile).getEdges else Edges.fromLines(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    logger.debug("STORING")
    edgesB.toFile(ofn)
    logger.debug("COMPLETE")
  }
}