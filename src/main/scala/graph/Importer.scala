package graph

object Importer {
  import EdgeUtils._
  import helper.Gauge.IteratorOperations

  def run(edgeFile: String, nShard: Int,
    selfloop: Boolean, bidirection: Boolean, uniq: Boolean) = {
    val shards = Shards(edgeFile, nShard)
    val edges0 = EdgeUtils.fromFile(edgeFile)
    val edgesL = if (selfloop) edges0 else edges0.filterNot(_.selfloop)
    val edgesB = if (bidirection) edgesL.toBidirection else edgesL
    val edgesU = if (uniq) edgesB.almostUniq else edgesB
    edgesU.foreachDo { e => shards.getShardByVertex(e.u).putEdge(e) }
    shards.putEdgeComplete
  }
}
