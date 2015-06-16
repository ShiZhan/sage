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
  import Edges._

  def run(edgeFileName: String, selfloop: Boolean, bidirectional: Boolean, binary: Boolean) = {
    val edges0 = if (binary) EdgeFile(edgeFileName).getEdges else Edges.fromLines(edgeFileName)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    val outFileName = s"imported-$edgeFileName"
    logger.debug("STORING")
    edgesB.toFile(outFileName)
    logger.debug("COMPLETE")
  }
}