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
  private val data = (0 to (nShard - 1)).map(shardName).map(Shard)
  private val flag = new BitSet()

  def intact = data.forall(_.exists)

  def getAllShards = data.toIterator
  def getAllEdges = data.toIterator.flatMap { _.getEdges }

  private def vertex2shardId(v: Long) = (v & (nShard - 1)).toInt

  def selectShard(id: Int) = data(id)
  def selectShardByVertex(vertex: Long) = data(vertex2shardId(vertex))

  def setUpdateFlag(id: Int) = flag.add(id)
  def setUpdateFlagByVertex(vertex: Long) = flag.add(vertex2shardId(vertex))
  def resetUpdateFlag(id: Int) = flag.remove(id)

  def getUpdatedShards = flag.map(data).toIterator

  def close = data.foreach(_.close) // only when opened as output stream (importer)
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