package graph

case class Shard(name: String) {
  import java.io.{ BufferedOutputStream, File, FileOutputStream }
  import scala.io.Source
  import EdgeUtils.Bytes2Edge

  val file = new File(name)
  def exists = file.exists()

  lazy val oStream = new BufferedOutputStream(new FileOutputStream(file))
  def putEdgeComplete = oStream.close()
  def putEdge(edge: Edge) = oStream.write(edge.toBytes)
  def putEdges(edges: Iterator[Edge]) = edges.foreach(putEdge)
  def getEdges =
    Source.fromFile(file, "ISO-8859-1").map(_.toByte).grouped(16).map(_.toArray.toEdge)
}

abstract class Shards(prefix: String, nShard: Int) {
  require(helper.Miscs.isPowerOf2(nShard))
  import scala.collection.mutable.BitSet

  private def shardName(id: Int) = "%s-%04x.bin".format(prefix, id)
  private val data = (0 to (nShard - 1)).map(shardName).map(Shard)
  private val flag = new BitSet()

  def size = nShard
  def intact = data.forall(_.exists)

  def getAllShards = data.toIterator
  def getAllEdges = data.toIterator.flatMap { _.getEdges }

  def vertex2shardId(v: Long) = (v & (nShard - 1)).toInt

  def getShard(id: Int) = data(id)
  def getShardByVertex(vertex: Long) = data(vertex2shardId(vertex))

  def setAllFlags = (0 to (nShard - 1)).foreach(flag.add)
  def setFlag(id: Int) = flag.add(id)
  def setFlagByVertex(vertex: Long) = flag.add(vertex2shardId(vertex))

  def getFlagString = flag.mkString("[", ", ", "]")
  def getFlagedShards = flag.toIterator.map { i => flag.remove(i); data(i) }
  def getFlagedEdges = flag.toIterator.flatMap { i => flag.remove(i); data(i).getEdges }

  def putEdge(e: Edge) = getShardByVertex(e.u).putEdge(e)
  def putEdgeComplete = data.foreach(_.putEdgeComplete)
}

class SimpleShards(prefix: String, nShard: Int) extends Shards(prefix, nShard)

class DoubleShards(prefix: String, nShard: Int) extends Shards(prefix, nShard) {
  val reverse = new SimpleShards(s"$prefix-r", nShard)
  override def intact = super.intact && reverse.intact
  override def putEdge(e: Edge) = { super.putEdge(e); reverse.putEdge(e.reverse) }
  override def putEdgeComplete = { super.putEdgeComplete; reverse.putEdgeComplete }
}