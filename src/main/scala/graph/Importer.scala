package graph

object Importer {
  import EdgeUtils.line2edge
  import helper.Gauge.IteratorOperations
  import helper.GetLines

  def run(edgeFile: String, nShard: Int) = {
    val shards = Shards(edgeFile, nShard)
    val edges = GetLines.fromFileOrConsole(edgeFile).map(line2edge).filter(_.valid)

    edges.foreachDo { e => shards.getShardByVertex(e.u).putEdge(e) }
    shards.close
  }
}
