package graph

object Importer extends helper.Logging {
  import Edges.EdgesWrapper

  implicit class RemoveConsecutiveDuplicates[T](seq: Iterator[T]) {
    var prev = None.asInstanceOf[T]
    def removeConsecutiveDuplicates =
      seq.filter { elem => if (elem != prev) { prev = elem; true } else false }
  }

  def run(edgeFile: String, selfloop: Boolean, bidirectional: Boolean,
    sort: Boolean, uniq: Boolean, binary: Boolean) = {
    val ofn = if (edgeFile.isEmpty) "graph.bin" else edgeFile + ".bin"
    val edges0 = if (binary) Edges.fromFile(edgeFile).all else Edges.fromLines(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirectional) edgesL.flatMap { e => Iterator(e, e.reverse) } else edgesL
    if (sort) {
      val edgesS = edgesB.sort
      val edgesU = if (uniq) edgesS.removeConsecutiveDuplicates else edgesS
      logger.info("STORING")
      edgesU.toFile(ofn)
      logger.info("COMPLETE")
    } else {
      logger.info("START")
      edgesB.toFile(ofn)
      logger.info("COMPLETE")
    }
  }
}