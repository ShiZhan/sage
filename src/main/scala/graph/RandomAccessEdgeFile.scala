package graph

/**
 * @author Zhan
 * SimpleEdgeFile:   randomly or sequentially access simple edges
 * WeightedEdgeFile: randomly or sequentially access weighted edges
 */
class SimpleEdgeFile(edgeFileName: String) extends EdgeFile(edgeFileName) {
  import java.nio.{ ByteBuffer, ByteOrder }
  import Edges.edgeSize
  import helper.IteratorOps.VisualOperations

  def close = fc.close()
  def total = fc.size() / edgeSize

  def putEdge(edge: SimpleEdge) = edge match {
    case Edge(u, v) =>
      buf.clear()
      buf.putLong(u)
      buf.putLong(v)
      buf.flip()
      fc.write(buf)
  }

  override def putEdges(edges: Iterator[SimpleEdge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (Edge(u, v) <- g) { buf.putLong(u); buf.putLong(v) }
      buf.flip()
      while (buf.hasRemaining) fc.write(buf)
    }
  }

  def putThenClose(edges: Iterator[SimpleEdge]) = super.putEdges(edges)

  def putRange(edges: Iterator[SimpleEdge], start: Long) = {
    fc.position(start * edgeSize)
    putEdges(edges)
  }

  def putRange(edges: Iterator[SimpleEdge], start: Long, count: Long) = {
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
      Iterator.continually { Edge(buf.getLong, buf.getLong) }.take(nBuf)
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
      Iterator.continually { Edge(buf.getLong, buf.getLong) }.take(nBuf)
    }.takeWhile(!_.isEmpty).flatten.takeWhile { _ => n -= 1; n >= 0 }
  }

  def getThenClose = super.getEdges
}

class WeightedEdgeFile(edgeFileName: String) extends WEdgeFile(edgeFileName) {
  import java.nio.{ ByteBuffer, ByteOrder }
  import WEdges.edgeSize
  import helper.IteratorOps.VisualOperations

  def close = fc.close()
  def total = fc.size() / edgeSize

  def putEdge(edge: WeightedEdge) = edge match {
    case Edge(u, v, w) =>
      buf.clear()
      buf.putLong(u)
      buf.putLong(v)
      buf.putFloat(w)
      buf.flip()
      fc.write(buf)
  }

  override def putEdges(edges: Iterator[WeightedEdge]) = {
    fc.position(0)
    edges.grouped(gSize).foreachDoWithScale(gScale) { g =>
      buf.clear()
      for (Edge(u, v, w) <- g) { buf.putLong(u); buf.putLong(v); buf.putFloat(w) }
      buf.flip()
      while (buf.hasRemaining) fc.write(buf)
    }
  }

  def putThenClose(edges: Iterator[WeightedEdge]) = super.putEdges(edges)

  def putRange(edges: Iterator[WeightedEdge], start: Long) = {
    fc.position(start * edgeSize)
    putEdges(edges)
  }

  def putRange(edges: Iterator[WeightedEdge], start: Long, count: Long) = {
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
      Iterator.continually { Edge(buf.getLong, buf.getLong, buf.getFloat) }.take(nBuf)
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
      Iterator.continually { Edge(buf.getLong, buf.getLong, buf.getFloat) }.take(nBuf)
    }.takeWhile(!_.isEmpty).flatten.takeWhile { _ => n -= 1; n >= 0 }
  }

  def getThenClose = super.getEdges
}
