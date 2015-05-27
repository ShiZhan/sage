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

  case class fromFile(edgeFile: String) {
    val p = Paths.get(edgeFile)
    val fc = FileChannel.open(p, READ)
    val buf = ByteBuffer.allocate(edgeSize).order(ByteOrder.LITTLE_ENDIAN)

    def close = fc.close()
    def total = fc.size() >> edgeScale
    def all = {
      fc.position(0)
      Iterator.continually {
        val nBytes = fc.read(buf)
        if (nBytes == edgeSize) {
          buf.flip()
          val u = buf.getLong
          val v = buf.getLong
          buf.clear()
          Some(Edge(u, v))
        } else {
          None
        }
      }.takeWhile(_ != None).map(_.get)
    }

    def range(start: Long, count: Long) = {
      var n = count
      fc.position(start << edgeScale)
      Iterator.continually {
        n -= 1
        val nBytes = fc.read(buf)
        if (nBytes == edgeSize) {
          buf.flip()
          val u = buf.getLong
          val v = buf.getLong
          buf.clear()
          Edge(u, v)
        } else {
          Edge(-1, -1)
        }
      }.takeWhile(_ => n >= 0)
    }
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