package graph

object Importer extends helper.Logging {
  import EdgeUtils._
  import helper.Gauge.IteratorOperations

  def run(edgeFile: String, nShard: Int, selfloop: Boolean, reverse: Boolean) = {
    val edges0 = fromFile(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val prefix = if (edgeFile.isEmpty) "graph" else edgeFile
    val shards = if (reverse) new DoubleShards(prefix, nShard) else new SimpleShards(prefix, nShard)
    logger.info("START")
    edgesL.foreachDo(shards.putEdge)
    shards.putEdgeComplete
    logger.info("COMPLETE")
  }
}