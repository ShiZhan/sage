package graph

object Importer {
  import EdgeUtils._
  import helper.Gauge.IteratorOperations

  def run(edgeFile: String, nShard: Int, selfloop: Boolean, reverse: Boolean) = {
    val edges0 = fromFile(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val shards = if (reverse) new DoubleShards(edgeFile, nShard) else new SimpleShards(edgeFile, nShard)
    edgesL.foreachDo(shards.putEdge)
    shards.putEdgeComplete
  }
}
