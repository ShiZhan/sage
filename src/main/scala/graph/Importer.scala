package graph

object Importer {
  import EdgeUtils._
  import helper.Gauge.IteratorOperations

  def run(edgeFile: String, nShard: Int,
    selfloop: Boolean, uniq: Boolean, reverse: Boolean) = {
    val edges0 = fromFile(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesU = if (uniq) edgesL.almostUniq else edgesL
    val shards = if (reverse) new DoubleShards(edgeFile, nShard) else new SimpleShards(edgeFile, nShard)
    edgesU.foreachDo(shards.putEdge)
    shards.putEdgeComplete
  }
}
