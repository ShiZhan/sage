package graph

/**
 * @author Zhan
 * Containers for WeightedEdge
 * WEdgeConsole: access edges from/to console
 * WEdgeText:    access edges from/to text files
 * WEdgeFile:    access edges from/to binary files
 * WEdges:       common values and factory functions for weighted edges
 */
class WEdgeConsole extends EdgeProvider[WeightedEdge] with EdgeStorage[WeightedEdge] {
  def putEdges(edges: Iterator[WeightedEdge]) = edges.foreach(println)

  def getEdges =
    io.Source.fromInputStream(System.in).getLines
      .map(WEdges.line2edge).filter(_ != None).map(_.get)
}

class WEdgeText(edgeFileName: String) extends EdgeProvider[WeightedEdge] with EdgeStorage[WeightedEdge] {
  import java.io.{ File, PrintWriter }
  import helper.IteratorOps.VisualOperations

  val file = new File(edgeFileName)

  def putEdges(edges: Iterator[WeightedEdge]) = {
    val pw = new PrintWriter(file)
    edges.map(_.toString).foreachDo(pw.println)
    pw.close()
  }

  def getEdges =
    io.Source.fromFile(file).getLines.map(WEdges.line2edge).filter(_ != None).map(_.get)
}

class WEdgeFile(edgeFileName: String) extends EdgeProvider[WeightedEdge] with EdgeStorage[WeightedEdge] {
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

  def putEdges(edges: Iterator[WeightedEdge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (Edge(u, v, w) <- g) buf.putInt(u).putInt(v).putFloat(w)
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
      Iterator.continually { Edge(buf.getInt, buf.getInt, buf.getFloat) }.take(nBuf)
    }.takeWhile { i => if (i.isEmpty) { fc.close(); false } else true }.flatten
  }
}

object WEdges extends helper.Logging {
  import scala.util.Random

  val edgeSize = 12 // Int + Int + Float

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); None
    case from :: to :: Nil =>
      Some(Edge(from.toInt, to.toInt, Random.nextFloat))
    case from :: to :: weight :: Nil =>
      Some(Edge(from.toInt, to.toInt, weight.toFloat))
    case _ =>
      logger.error("invalid: [{}]", line); None
  }

  def fromFile(edgeFileName: String) =
    if (edgeFileName.isEmpty) new WEdgeFile("graph.bin") else new WEdgeFile(edgeFileName)

  def fromText(edgeFileName: String) =
    if (edgeFileName.isEmpty) new WEdgeText("graph.edges") else new WEdgeText(edgeFileName)

  def fromConsole = new WEdgeConsole
}
