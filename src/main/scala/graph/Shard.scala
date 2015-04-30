package graph

case class Shard(name: String) {
  import java.io.{ BufferedOutputStream, File, FileOutputStream }
  import scala.io.Source
  import EdgeUtils.Bytes2Edge

  val file = new File(name)
  def exists = file.exists()

  lazy val oStream = new BufferedOutputStream(new FileOutputStream(file))
  lazy val iStream = Source.fromFile(file, "ISO-8859-1").map(_.toByte)
  def close = oStream.close()

  def putEdge(edge: Edge) = oStream.write(edge.toBytes)
  def putEdges(edges: Iterator[Edge]) = edges.foreach { e => oStream.write(e.toBytes) }
  def getEdges = iStream.grouped(16).map(_.toArray.toEdge)
}

class Shards(prefix: String, nShard: Int) {
  import scala.collection.mutable.BitSet

  private def shardName(id: Int) = "%s-%03d.bin".format(prefix, id)
  private val shardArray = (0 to (nShard - 1)).map(shardName).map(Shard).toArray
  private val shard2Update = new BitSet()

  private def vertex2shardId(v: Long) = (v & (nShard - 1)).toInt

  def getArray = shardArray
  def intact = shardArray.forall(_.exists)
  def selectByVertex(vertex: Long) = shardArray(vertex2shardId(vertex))

  def setUpdateFlagByVertex(vertex: Long) = shard2Update.add(vertex2shardId(vertex))
  def resetUpdateFlag(id: Int) = shard2Update.remove(id)

  def close = shardArray.foreach(_.close) // only when opened as output stream (importer)
}

object Shards {
  def apply(prefix: String, nShard: Int) = new Shards(prefix, nShard)

  implicit class nShardShouldBePowerOf2(i: Int) {
    require(i > 0)
    private def isPowerOf2(i: Int) = ((i - 1) & i) == 0
    def toPowerOf2 = {
      def highestBit(remainder: Int, c: Int): Int =
        if (remainder > 0) highestBit(remainder >> 1, c + 1) else c
      require(i <= (1 << 30))
      val n = if (isPowerOf2(i)) i else 1 << highestBit(i, 0)
      assert(n >= i && i * 2 > n && isPowerOf2(n))
      n
    }
  }
}