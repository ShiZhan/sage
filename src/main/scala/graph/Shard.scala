package graph

class Shard(name: String) {
  import java.io.{ BufferedOutputStream, File, FileOutputStream }
  import scala.io.Source
  import EdgeUtils.Bytes2Edge

  val file = new File(name)
  def exists = file.exists()

  lazy val oStream = new BufferedOutputStream(new FileOutputStream(file))
  def putEdgeComplete = oStream.close()
  def putEdge(edge: Edge) = oStream.write(edge.toBytes)
  def putEdges(edges: Iterator[Edge]) = edges.foreach(putEdge)
  def getEdges = Source.fromFile(file, "ISO-8859-1").map(_.toByte)
    .grouped(16).map(_.toArray.toEdge)
}

class Shards(prefix: String, nShard: Int) {
  require(helper.Miscs.isPowerOf2(nShard))
  def genShardName(id: Int) = "%s-%04x.bin".format(prefix, id)
  val data = (0 to (nShard - 1)).map(genShardName).map(new Shard(_))

  def size = nShard
  def intact = data.forall(_.exists)

  def getAllShards = data.toIterator
  def getAllEdges = data.toIterator.flatMap { _.getEdges }

  def vertex2shard(v: Long) = (v & (nShard - 1)).toInt
  def getShard(id: Int) = data(id)
  def getShardByVertex(v: Long) = data(vertex2shard(v))

  def putEdge(e: Edge) = getShardByVertex(e.u).putEdge(e)
  def putEdgeComplete = data.foreach(_.putEdgeComplete)
}

class DirectionalShards(prefix: String, nShard: Int) extends Shards(prefix, nShard) {
  import scala.collection.mutable.BitSet

  val flag = new BitSet()
  def getFlagTotal = flag.size
  def getFlagState = "Shards: % 5d (% 4d%% )".format(getFlagTotal, 100 * getFlagTotal / nShard)

  def setAllFlags = (0 to (nShard - 1)).foreach(flag.add)
  def setFlag(id: Int) = flag.add(id)
  def setFlagByVertex(v: Long) = flag.add(vertex2shard(v))

  def getFlagedShards = flag.toIterator.map { i => flag.remove(i); data(i) }
  def getFlagedEdges = flag.toIterator.flatMap { i => flag.remove(i); data(i).getEdges }
}

class BidirectionalShards(prefix: String, nShard: Int) extends DirectionalShards(prefix, nShard) {
  val reverse = new DirectionalShards(s"$prefix-r", nShard)
  override def size = nShard * 2
  override def intact = super.intact && reverse.intact
  override def putEdge(e: Edge) = { super.putEdge(e); reverse.putEdge(e.reverse) }
  override def putEdgeComplete = { super.putEdgeComplete; reverse.putEdgeComplete }

  def getAllReversedShards = reverse.getAllShards
  def getAllReversedEdges = reverse.getAllEdges

  override def getFlagedShards =
    flag.toIterator.flatMap { i => flag.remove(i); Iterator(data(i), reverse.data(i)) }
  override def getFlagedEdges =
    flag.toIterator.flatMap { i => flag.remove(i); data(i).getEdges ++ reverse.data(i).getEdges }
  override def getFlagTotal = flag.size * 2
  override def getFlagState = "Shards: % 5d (% 4d%% )".format(getFlagTotal, 100 * getFlagTotal / nShard)
}