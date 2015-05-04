package graph

object Importer {
  import EdgeUtils.line2edge
  import helper.Gauge.IteratorOperations
  import helper.{ GetLines, BloomFilter }

  implicit class AlmostUniqIterator[T](iterator: Iterator[T]) {
    val bitsSize = 256 * 1024 * 1024 // 32MB
    val exptSize = 128 * 1024 * 1024 // almost ensure no duplicate items in 128M range
    val bf = new BloomFilter(bitsSize, exptSize)
    var counter = 0L
    def almostUniq = iterator.map { i =>
      if (bf.contains(i))
        None
      else {
        if (counter > exptSize) {
          bf.bitArray.clear
          counter = 0L
        } else {
          bf.add(i)
          counter += 1L
        }
        Some(i)
      }
    }.filter(_ != None).map(_.get)
  }

  implicit class EdgeMirroring(edges: Iterator[Edge]) {
    def toBidirection = edges.flatMap { e => Seq(e, Edge(e.v, e.u)).toIterator }
  }

  def run(edgeFile: String, nShard: Int, uniq: Boolean, bidirection: Boolean) = {
    val shards = Shards(edgeFile, nShard)
    val edges0 = GetLines.fromFileOrConsole(edgeFile).map(line2edge).filter(_.valid)
    val edgesB = if (bidirection) edges0.toBidirection else edges0
    val edgesU = if (uniq) edgesB.almostUniq else edgesB

    edgesU.foreachDo { e => shards.getShardByVertex(e.u).putEdge(e) }

    shards.close
  }
}
