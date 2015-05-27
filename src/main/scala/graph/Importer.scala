package graph

object Importer extends helper.Logging {
  import Edges.EdgesWrapper

  def run(edgeFile: String, selfloop: Boolean, bidirectional: Boolean) = {
    val ofn = if (edgeFile.isEmpty) "graph.bin" else edgeFile + ".bin"
    val edges0 = Edges.fromTxt(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    logger.info("START")
    edgesB.toBin(ofn)
    logger.info("COMPLETE")
  }
}