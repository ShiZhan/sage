package graph

object Importer extends helper.Logging {
  import Edges._

//  def run(edgeFile: String, selfloop: Boolean, bidirectional: Boolean,
//    sort: Boolean, uniq: Boolean, binary: Boolean) = {
  def run(edgeFile: String, selfloop: Boolean, bidirectional: Boolean,
    uniq: Boolean, binary: Boolean) = {
    val ofn = if (edgeFile.isEmpty) "graph.bin" else edgeFile + ".bin"
    val edges0 = if (binary) EdgeFile(edgeFile).get else Edges.fromLines(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
//    if (sort) {
//      val edgesS = edgesB.mergeSort(1 << 22)
//      val edgesU = if (uniq) edgesS.uniq else edgesS
//      logger.info("STORING")
//      edgesU.toFile(ofn)
//      logger.info("COMPLETE")
//    } else {
//      logger.info("START")
//      edgesB.toFile(ofn)
//      logger.info("COMPLETE")
//    }
    edgesB.toFile(ofn)
  }
}