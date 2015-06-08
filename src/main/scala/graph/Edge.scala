package graph

/**
 * @author Zhan
 * Edge:     edge class
 * EdgeFile: file for storing edge list that can be accessed randomly or sequentially
 * Edges:    common values and converter functions
 */
case class Edge(u: Long, v: Long) extends Ordered[Edge] {
  def compare(that: Edge) =
    if (u != that.u) {
      if ((u - that.u) > 0) 1 else -1
    } else if (v != that.v) {
      if ((v - that.v) > 0) 1 else -1
    } else 0
  def selfloop = u == v
  def reverse = Edge(v, u)
  override def toString = s"$u $v"
}

case class EdgeFile(name: String) {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import Edges.{ edgeScale, edgeSize }
  import helper.IteratorOps.VisualOperations

  val gScale = 13
  val gSize = 1 << gScale
  val bSize = edgeSize << gScale
  val p = Paths.get(name)
  val fc = FileChannel.open(p, READ, WRITE, CREATE)
  val buf = ByteBuffer.allocate(bSize).order(ByteOrder.LITTLE_ENDIAN)

  def close = fc.close()
  def total = fc.size() >> edgeScale

  def putEdge(edge: Edge) = edge match {
    case Edge(u, v) =>
      buf.clear()
      buf.putLong(u)
      buf.putLong(v)
      buf.flip()
      fc.write(buf)
  }

  def put(edges: Iterator[Edge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (Edge(u, v) <- g) { buf.putLong(u); buf.putLong(v) }
      buf.flip()
      while (buf.hasRemaining) fc.write(buf)
    }
  }

  def putThenClose(edges: Iterator[Edge]) = { put(edges); fc.close() }

  def putRange(edges: Iterator[Edge], start: Long) = {
    fc.position(start << edgeScale)
    put(edges)
  }

  def putRange(edges: Iterator[Edge], start: Long, count: Long) = {
    var n = count
    fc.position(start << edgeScale)
    put(edges.takeWhile { _ => n -= 1; n >= 0 })
  }

  def get = {
    fc.position(0)
    Iterator.continually {
      buf.clear()
      while (fc.read(buf) != -1 && buf.hasRemaining) {}
      buf.flip()
      val nBuf = buf.remaining() >> edgeScale
      Iterator.continually { Edge(buf.getLong, buf.getLong) }.take(nBuf)
    }.takeWhile(!_.isEmpty).flatten
  }

  def getRange(start: Long, count: Long) = {
    var n = count
    fc.position(start << edgeScale)
    Iterator.continually {
      buf.clear()
      while (fc.read(buf) != -1 && buf.hasRemaining) {}
      buf.flip()
      val nBuf = buf.remaining() >> edgeScale
      Iterator.continually { Edge(buf.getLong, buf.getLong) }.take(nBuf)
    }.takeWhile(!_.isEmpty).flatten.takeWhile { _ => n -= 1; n >= 0 }
  }

  def getThenClose = {
    fc.position(0)
    Iterator.continually {
      buf.clear()
      while (fc.read(buf) != -1 && buf.hasRemaining) {}
      buf.flip()
      val nBuf = buf.remaining() >> edgeScale
      Iterator.continually { Edge(buf.getLong, buf.getLong) }.take(nBuf)
    }.takeWhile { i => if (i.isEmpty) { fc.close(); false } else true }.flatten
  }
}

object Edges extends helper.Logging {
  import helper.Lines
  import Lines.LinesWrapper

  val edgeScale = 4
  val edgeSize = 1 << edgeScale

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); None
    case from :: to :: Nil =>
      Some(Edge(from.toLong, to.toLong))
    case _ =>
      logger.error("invalid: [{}]", line); None
  }

  def fromLines(edgeFile: String) =
    Lines.fromFileOrConsole(edgeFile).map(line2edge).filter(_ != None).map(_.get)

  implicit class EdgesWrapper(edges: Iterator[Edge]) {
    def toText(edgeFile: String) = edges.map(_.toString).toFile(edgeFile)
    def toFile(edgeFile: String) = EdgeFile(edgeFile).putThenClose(edges)
  }
}