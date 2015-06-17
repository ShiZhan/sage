package graph

/**
 * @author Zhan
 * Edge:         edge class
 * EdgeStorage:  interface for edge storing classes
 * EdgeProvider: interface for edge loading classes
 * EdgeConsole:  access edges from/to console
 * EdgeText:     access edges from/to text files
 * EdgeFile:     access edges from/to binary files
 * RandomAccessEdgeFile: edge list file that can be accessed randomly or sequentially
 * Edges:        common values and factory functions
 */
case class WEdge(u: Long, v: Long, w: Float) extends Ordered[WEdge] {
  def compare(that: WEdge) =
    if (u != that.u) {
      if ((u - that.u) > 0) 1 else -1
    } else if (v != that.v) {
      if ((v - that.v) > 0) 1 else -1
    } else 0
  def selfloop = u == v
  def reverse = WEdge(v, u, w)
  override def toString = s"$u $v $w"
}

trait WEdgeStorage {
  def putEdges(edges: Iterator[WEdge])
}

trait WEdgeProvider {
  def getEdges: Iterator[WEdge]
}

class WEdgeConsole extends WEdgeProvider with WEdgeStorage {
  def putEdges(edges: Iterator[WEdge]) = edges.foreach(println)

  def getEdges =
    io.Source.fromInputStream(System.in).getLines
      .map(WEdges.line2edge).filter(_ != None).map(_.get)
}

class WEdgeText(edgeFileName: String) extends WEdgeProvider with WEdgeStorage {
  import java.io.{ File, PrintWriter }
  import helper.IteratorOps.VisualOperations

  val file = new File(edgeFileName)

  def putEdges(edges: Iterator[WEdge]) = {
    val pw = new PrintWriter(file)
    edges.map(_.toString).foreachDo(pw.println)
    pw.close()
  }

  def getEdges =
    io.Source.fromFile(file).getLines.map(WEdges.line2edge).filter(_ != None).map(_.get)
}

class WEdgeFile(edgeFileName: String) extends WEdgeProvider with WEdgeStorage {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import WEdges.edgeSize
  import helper.IteratorOps.VisualOperations

  val gScale = 13
  val gSize = 1 << gScale
  val bSize = edgeSize << gScale
  val p = Paths.get(edgeFileName)
  val fc = FileChannel.open(p, READ, WRITE, CREATE)
  val buf = ByteBuffer.allocate(bSize).order(ByteOrder.LITTLE_ENDIAN)

  def putEdges(edges: Iterator[WEdge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (WEdge(u, v, w) <- g) { buf.putLong(u); buf.putLong(v); buf.putFloat(w) }
      buf.flip()
      while (buf.hasRemaining) fc.write(buf)
    }
    fc.close()
  }

  def getEdges = {
    fc.position(0)
    Iterator.continually {
      buf.clear()
      while (fc.read(buf) != -1 && buf.hasRemaining) {}
      buf.flip()
      val nBuf = buf.remaining() / edgeSize
      Iterator.continually { WEdge(buf.getLong, buf.getLong, buf.getFloat) }.take(nBuf)
    }.takeWhile { i => if (i.isEmpty) { fc.close(); false } else true }.flatten
  }
}

class RandomAccessWEdgeFile(edgeFileName: String) extends WEdgeFile(edgeFileName) {
  import java.nio.{ ByteBuffer, ByteOrder }
  import Edges.edgeSize
  import helper.IteratorOps.VisualOperations

  def name = p.toString
  def close = fc.close()
  def total = fc.size() / edgeSize

  def putEdge(edge: WEdge) = edge match {
    case WEdge(u, v, w) =>
      buf.clear()
      buf.putLong(u)
      buf.putLong(v)
      buf.putFloat(w)
      buf.flip()
      fc.write(buf)
  }

  override def putEdges(edges: Iterator[WEdge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (WEdge(u, v, w) <- g) { buf.putLong(u); buf.putLong(v); buf.putFloat(w) }
      buf.flip()
      while (buf.hasRemaining) fc.write(buf)
    }
  }

  def putThenClose(edges: Iterator[WEdge]) = super.putEdges(edges)

  def putRange(edges: Iterator[WEdge], start: Long) = {
    fc.position(start * edgeSize)
    putEdges(edges)
  }

  def putRange(edges: Iterator[WEdge], start: Long, count: Long) = {
    fc.position(start * edgeSize)
    var n = count
    putEdges(edges.takeWhile { _ => n -= 1; n >= 0 })
  }

  override def getEdges = {
    fc.position(0)
    Iterator.continually {
      buf.clear()
      while (fc.read(buf) != -1 && buf.hasRemaining) {}
      buf.flip()
      val nBuf = buf.remaining() / edgeSize
      Iterator.continually { WEdge(buf.getLong, buf.getLong, buf.getFloat) }.take(nBuf)
    }.takeWhile(!_.isEmpty).flatten
  }

  def getRange(start: Long, count: Long) = {
    var n = count
    fc.position(start * edgeSize)
    Iterator.continually {
      buf.clear()
      while (fc.read(buf) != -1 && buf.hasRemaining) {}
      buf.flip()
      val nBuf = buf.remaining() / edgeSize
      Iterator.continually { WEdge(buf.getLong, buf.getLong, buf.getFloat) }.take(nBuf)
    }.takeWhile(!_.isEmpty).flatten.takeWhile { _ => n -= 1; n >= 0 }
  }

  def getThenClose = super.getEdges
}

object WEdges extends helper.Logging {
  val edgeSize = 20

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); None
    case from :: to :: weight :: Nil =>
      Some(WEdge(from.toLong, to.toLong, weight.toFloat))
    case _ =>
      logger.error("invalid: [{}]", line); None
  }

  def fromFile(edgeFileName: String) =
    if (edgeFileName.isEmpty) new WEdgeFile("graph.bin") else new WEdgeFile(edgeFileName)

  def fromText(edgeFileName: String) =
    if (edgeFileName.isEmpty) new WEdgeText("graph.edges") else new WEdgeText(edgeFileName)

  def fromConsole = new WEdgeConsole
}
