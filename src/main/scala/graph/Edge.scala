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

trait EdgeStorage {
  def putEdges(edges: Iterator[Edge])
}

trait EdgeProvider {
  def getEdges: Iterator[Edge]
}

class EdgeConsole extends EdgeProvider with EdgeStorage {
  def putEdges(edges: Iterator[Edge]) = edges.foreach(println)

  def getEdges =
    io.Source.fromInputStream(System.in).getLines
      .map(Edges.line2edge).filter(_ != None).map(_.get)
}

class EdgeText(edgeFileName: String) extends EdgeProvider with EdgeStorage {
  import java.io.{ File, PrintWriter }
  import helper.IteratorOps.VisualOperations

  val file = new File(edgeFileName)

  def putEdges(edges: Iterator[Edge]) = {
    val pw = new PrintWriter(file)
    edges.map(_.toString).foreachDo(pw.println)
    pw.close()
  }

  def getEdges =
    io.Source.fromFile(file).getLines.map(Edges.line2edge).filter(_ != None).map(_.get)
}

class EdgeFile(edgeFileName: String) extends EdgeProvider with EdgeStorage {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import Edges.{ edgeScale, edgeSize }
  import helper.IteratorOps.VisualOperations

  val gScale = 13
  val gSize = 1 << gScale
  val bSize = edgeSize << gScale
  val p = Paths.get(edgeFileName)
  val fc = FileChannel.open(p, READ, WRITE, CREATE)
  val buf = ByteBuffer.allocate(bSize).order(ByteOrder.LITTLE_ENDIAN)

  def putEdges(edges: Iterator[Edge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (Edge(u, v) <- g) { buf.putLong(u); buf.putLong(v) }
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
      val nBuf = buf.remaining() >> edgeScale
      Iterator.continually { Edge(buf.getLong, buf.getLong) }.take(nBuf)
    }.takeWhile { i => if (i.isEmpty) { fc.close(); false } else true }.flatten
  }
}

class RandomAccessEdgeFile(edgeFileName: String) extends EdgeFile(edgeFileName) {
  import java.nio.{ ByteBuffer, ByteOrder }
  import Edges.{ edgeScale, edgeSize }
  import helper.IteratorOps.VisualOperations

  def name = p.toString
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

  override def putEdges(edges: Iterator[Edge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (Edge(u, v) <- g) { buf.putLong(u); buf.putLong(v) }
      buf.flip()
      while (buf.hasRemaining) fc.write(buf)
    }
  }

  def putThenClose(edges: Iterator[Edge]) = super.putEdges(edges)

  def putRange(edges: Iterator[Edge], start: Long) = {
    fc.position(start << edgeScale)
    putEdges(edges)
  }

  def putRange(edges: Iterator[Edge], start: Long, count: Long) = {
    fc.position(start << edgeScale)
    var n = count
    putEdges(edges.takeWhile { _ => n -= 1; n >= 0 })
  }

  override def getEdges = {
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

  def getThenClose = super.getEdges
}

object Edges extends helper.Logging {
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

  def fromFile(edgeFileName: String) =
    if (edgeFileName.isEmpty) new EdgeFile("graph.bin") else new EdgeFile(edgeFileName)

  def fromText(edgeFileName: String) =
    if (edgeFileName.isEmpty) new EdgeText("graph.edges") else new EdgeText(edgeFileName)

  def fromConsole = new EdgeConsole
}