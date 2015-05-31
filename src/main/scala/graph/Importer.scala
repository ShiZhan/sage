package graph

object Importer extends helper.Logging {
  import Edges._

  def run(edgeFile: String, selfloop: Boolean, bidirectional: Boolean, binary: Boolean) = {
    val ofn = if (edgeFile.isEmpty) "graph.bin" else edgeFile + ".bin"
    val edges0 = if (binary) EdgeFile(edgeFile).get else Edges.fromLines(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    logger.info("STORING")
    edgesB.toFile(ofn)
    logger.info("COMPLETE")
  }
}