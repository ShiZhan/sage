package graph

/**
 * @author Zhan
 * Containers for SimpleEdge
 * EdgeConsole:  access edges from/to console
 * EdgeText:     access edges from/to text files
 * EdgeFile:     access edges from/to binary files
 * Edges:        common values and factory functions
 */
class EdgeConsole extends EdgeProvider[SimpleEdge] with EdgeStorage[SimpleEdge] {
  def putEdges(edges: Iterator[SimpleEdge]) = edges.foreach(println)

  def getEdges =
    io.Source.fromInputStream(System.in).getLines
      .map(Edges.line2edge).filter(_ != None).map(_.get)
}

class EdgeText(edgeFileName: String) extends EdgeProvider[SimpleEdge] with EdgeStorage[SimpleEdge] {
  import java.io.{ File, PrintWriter }
  import helper.IteratorOps.VisualOperations

  val file = new File(edgeFileName)

  def putEdges(edges: Iterator[SimpleEdge]) = {
    val pw = new PrintWriter(file)
    edges.map(_.toString).foreachDo(pw.println)
    pw.close()
  }

  def getEdges =
    io.Source.fromFile(file).getLines.map(Edges.line2edge).filter(_ != None).map(_.get)
}

class EdgeFile(edgeFileName: String) extends EdgeProvider[SimpleEdge] with EdgeStorage[SimpleEdge] {
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

  def putEdges(edges: Iterator[SimpleEdge]) = {
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
