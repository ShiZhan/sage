package graph

case class Edge(u: Long, v: Long) {
  import java.nio.{ ByteBuffer, ByteOrder }

  def selfloop = u == v
  def reverse = Edge(v, u)
  def toBytes =
    ByteBuffer.allocate(Edges.edgeSize).order(ByteOrder.LITTLE_ENDIAN)
      .putLong(u).putLong(v).array()
  override def toString = s"$u $v"
}

object Edges extends helper.Logging {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import java.io.{ BufferedOutputStream, File, FileOutputStream }

  val edgeScale = 4
  val edgeSize = 1 << edgeScale

  def bytes2edge(bytes: Array[Byte]) = {
    require(bytes.length == edgeSize)
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    Edge(buf.getLong, buf.getLong)
  }

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); None
    case from :: to :: Nil =>
      Some(Edge(from.toLong, to.toLong))
    case _ =>
      logger.error("invalid: [{}]", line); None
  }

  def fromLines(edgeFile: String) =
    helper.Lines.fromFileOrConsole(edgeFile)
      .map(line2edge).filter(_ != None).map(_.get)

  def fromFile(edgeFile: String) =
    io.Source.fromFile(new File(edgeFile), "ISO-8859-1")
      .map(_.toByte).grouped(edgeSize).map(_.toArray).map(bytes2edge)

  def fromBinRange(edgeFile: String, start: Long, number: Long) = {
    val p = Paths.get(edgeFile)
    val fc = FileChannel.open(p, READ)
    val buf = ByteBuffer.allocate(edgeSize).order(ByteOrder.LITTLE_ENDIAN)
    var n = number
    fc.position(start << edgeScale)
    Iterator.continually {
      n -= 1
      if (n >= 0) {
        fc.read(buf)
        buf.flip()
        val u = buf.getLong
        val v = buf.getLong
        buf.clear()
        Edge(u, v)
      } else {
        fc.close()
        Edge(-1, -1)
      }
    }.takeWhile(_ => n >= 0)
  }

  def total(edgeFile: String) = {
    val p = Paths.get(edgeFile)
    val fc = FileChannel.open(p, READ)
    val size = fc.size()
    fc.close()
    size >> edgeScale
  }

  implicit class EdgesWrapper(edges: Iterator[Edge]) {
    import helper.Gauge.IteratorOperations

    def toBin(edgeFile: String) = {
      val os = new BufferedOutputStream(new FileOutputStream(new File(edgeFile)))
      edges.foreachDo { e => os.write(e.toBytes) }
      os.close()
    }
  }
}