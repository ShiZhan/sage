package graph

class Shard(prefix: String, id: Int) {
  import java.io.{ BufferedOutputStream, File, FileOutputStream }
  import scala.io.Source
  import EdgeConverters.Bytes2Edge

  val name = "%s-%03d.bin".format(prefix, id)
  val file = new File(name)
  def exists = file.exists()

  lazy val oStream = new BufferedOutputStream(new FileOutputStream(file))
  lazy val iStream = Source.fromFile(file, "ISO-8859-1").map(_.toByte)

  def close = oStream.close()

  def putEdges(edges: Iterator[Edge]) = edges.foreach { e => oStream.write(e.toBytes) }
  def putEdge(edge: Edge) = oStream.write(edge.toBytes)
  def getEdges = iStream.grouped(16).map(_.toArray.toEdge)

  override def toString = name
}

object Shards {
  def apply(prefix: String, id: Int) = new Shard(prefix, id)
  def init(prefix: String, nShard: Int) =
    (0 to (nShard - 1)).map { Shards(prefix, _) }.toArray
}
