package graph

object Importer {
  import EdgeUtils.line2edge
  import helper.Gauge.IteratorOperations
  import helper.{ GetLines, BloomFilter }

  def _runUniq(edges: Iterator[Edge], shards: Shards, bidirection: Boolean) = {
    val bitsSize = 128 * 1024 * 1024 // 32MB each, 256 MB if 16 shards, may increase heap size to run
    val exptSize = 64 * 1024 * 1024 // expect every shard store at most 64M edges
    val filters = Array.fill(shards.size)(new BloomFilter(bitsSize, exptSize))
    edges.foreachDo { e =>
      val Edge(u, v) = e
      val uShardId = shards.vertex2shardId(u)

      if (!filters(uShardId).contains(e)) {
        shards.getShard(uShardId).putEdge(e)
        filters(uShardId).add(e)
      }

      if (bidirection) {
        val eReverse = Edge(v, u)
        val vShardId = shards.vertex2shardId(v)

        if (!filters(vShardId).contains(eReverse)) {
          shards.getShard(vShardId).putEdge(eReverse)
          filters(vShardId).add(eReverse)
        }
      }
    }
  }

  def _run(edges: Iterator[Edge], shards: Shards, bidirection: Boolean) =
    edges.foreachDo { e =>
      val Edge(u, v) = e
      shards.getShardByVertex(u).putEdge(e)
      if (bidirection) shards.getShardByVertex(v).putEdge(Edge(v, u))
    }

  def run(edgeFile: String, nShard: Int, uniq: Boolean, bidirection: Boolean) = {
    val shards = Shards(edgeFile, nShard)
    val edges = GetLines.fromFileOrConsole(edgeFile).map(line2edge).filter(_.valid)
    if (uniq)
      _runUniq(edges, shards, bidirection)
    else
      _run(edges, shards, bidirection)
    shards.close
  }
}
