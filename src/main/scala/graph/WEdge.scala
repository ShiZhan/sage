package graph

/**
 * @author Zhan
 * WEdge:        weighted edge class
 * WEdgeConsole: access edges from/to console
 * WEdgeText:    access edges from/to text files
 * WEdgeFile:    access edges from/to binary files
 * WEdges:       common values and factory functions
 */
case class WEdge(u: Long, v: Long, w: Float) extends EdgeBase(u, v) {
  def selfloop = u == v
  def reverse = WEdge(v, u, w)
  override def toString = s"$u $v $w"
}

class WEdgeConsole extends EdgeProvider[WEdge] with EdgeStorage[WEdge] {
  def putEdges(edges: Iterator[WEdge]) = edges.foreach(println)

  def getEdges =
    io.Source.fromInputStream(System.in).getLines
      .map(WEdges.line2edge).filter(_ != None).map(_.get)
}

class WEdgeText(edgeFileName: String) extends EdgeProvider[WEdge] with EdgeStorage[WEdge] {
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

class WEdgeFile(edgeFileName: String) extends EdgeProvider[WEdge] with EdgeStorage[WEdge] {
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

object WEdges extends helper.Logging {
  val edgeSize = 20 // Long + Long + Float

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
