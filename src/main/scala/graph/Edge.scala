package graph

/**
 * @author Zhan
 * Edge:         edge class
 * EdgeStorage:  interface for edge storing classes
 * EdgeProvider: interface for edge loading classes
 * EdgeConsole:  access edges from/to console
 * EdgeText:     access edges from/to text files
 * EdgeFile:     access edges from/to binary files
 * Edges:        common values and factory functions
 */
abstract class EdgeBase[E](u: Long, v: Long) {
  def selfloop = u == v
  def reverse: E
  override def toString = s"$u $v"
}

case class Edge(u: Long, v: Long) extends EdgeBase[Edge](u, v) {
  def reverse = Edge(v, u)
}

trait EdgeStorage[E <: EdgeBase[E]] {
  def putEdges(edges: Iterator[E])
}

trait EdgeProvider[E <: EdgeBase[E]] {
  def getEdges: Iterator[E]
}

class EdgeConsole extends EdgeProvider[Edge] with EdgeStorage[Edge] {
  def putEdges(edges: Iterator[Edge]) = edges.foreach(println)

  def getEdges =
    io.Source.fromInputStream(System.in).getLines
      .map(Edges.line2edge).filter(_ != None).map(_.get)
}

class EdgeText(edgeFileName: String) extends EdgeProvider[Edge] with EdgeStorage[Edge] {
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

class EdgeFile(edgeFileName: String) extends EdgeProvider[Edge] with EdgeStorage[Edge] {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import Edges.edgeSize
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
      val nBuf = buf.remaining() / edgeSize
      Iterator.continually { Edge(buf.getLong, buf.getLong) }.take(nBuf)
    }.takeWhile { i => if (i.isEmpty) { fc.close(); false } else true }.flatten
  }
}

object Edges extends helper.Logging {
  val edgeSize = 16 // 2 Long = 16 Bytes

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); None
    case from :: to :: tail =>
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