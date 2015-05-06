package graph

object Importer {
  import helper.Gauge.IteratorOperations

  def run(edgeFile: String, nShard: Int) = {
    val shards = Shards(edgeFile, nShard)
    EdgeUtils.fromFile(edgeFile).foreachDo { e => shards.getShardByVertex(e.u).putEdge(e) }
    shards.close
  }
}
