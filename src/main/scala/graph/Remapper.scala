package graph

class Remapper(mapFN: String) {
  import EdgeUtils.line2edge
  import helper.GetLines

  private val vMap = GetLines.fromFile(mapFN)
    .zipWithIndex.map { case (str, idx) => (str.toLong, idx.toLong) }.toMap

  private def mapEdge(e: Edge) = e match {
    case Edge(u, v) => Edge(vMap.getOrElse(u, u), vMap.getOrElse(v, v))
  }

  def remapCSV(edgeFN: String) =
    GetLines.fromFileOrConsole(edgeFN)
      .map(line2edge).filter(_.valid)
      .map(mapEdge).foreach(println)

  def remapBIN(shardFN: String) = {
    val edgesMapped = Shard(shardFN).getEdges.map(mapEdge)
    Shard(shardFN + "-mapped.bin").putEdges(edgesMapped)
  }
}

object Remapper {
  def run(edgeFile: String, mapFile: String) = new Remapper(mapFile).remapCSV(edgeFile)
}