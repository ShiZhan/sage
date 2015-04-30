package graph

class Remapper(mapFN: String) extends helper.Logging {
  import EdgeUtils.line2edge
  import helper.GetLines

  private val vMap = GetLines.fromFile(mapFN)
    .zipWithIndex.map { case (str, idx) => (str.toLong, idx.toLong) }.toMap

  def remapCSV(edgeFN: String) = {
    logger.info("remapping CSV [{}]", edgeFN)
    val edges = GetLines.fromFileOrConsole(edgeFN).map(line2edge).filter(_.valid)
    val edgesMapped = edges.map { case Edge(u, v) => Edge(vMap.getOrElse(u, u), vMap.getOrElse(v, v)) }
    edgesMapped.foreach(println)
  }

  def remapBIN(shardFN: String) = {
    logger.info("remapping BIN [{}]", shardFN)
    val edges = Shard(shardFN).getEdges
    val edgesMapped = edges.map { case Edge(u, v) => Edge(vMap.getOrElse(u, u), vMap.getOrElse(v, v)) }
    val shardMapped = Shard(shardFN + "-mapped.bin")
    shardMapped.putEdges(edgesMapped)
  }
}

object Remapper {
  def run(inFile: String, mapFile: String) =
    new Remapper(mapFile).remapCSV(inFile)
}