package graph

import java.nio.{ ByteBuffer, ByteOrder }

case class Edge(u: Long, v: Long) {
  def selfloop = u == v

  def toBytes =
    ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(u).putLong(v).array()

  override def toString = s"$u $v"
}

object EdgeUtils extends helper.Logging {
  import helper.Lines
  import Lines.Lines2File

  implicit class Bytes2Edge(bytes: Array[Byte]) {
    require(bytes.length == 16)

    private val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    def toEdge = Edge(buf.getLong, buf.getLong)
  }

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); None
    case from :: to :: Nil =>
      Some(Edge(from.toLong, to.toLong))
    case _ =>
      logger.error("invalid: [{}]", line); None
  }

  def fromFile(edgeFile: String) =
    Lines.fromFileOrConsole(edgeFile).map(line2edge).filter(_ != None).map(_.get)

  implicit class EdgeMirroring(edges: Iterator[Edge]) {
    def toBidirection = edges.flatMap { e => Iterator(e, Edge(e.v, e.u)) }
  }

  implicit class AlmostUniqIterator[T](iterator: Iterator[T]) {
    import scala.collection.mutable.Set
    val sSize = 1024 * 1024 // 16 MB each
    val aSize = 16 // 256 MB total, a 16 M items window for checking duplicates
    val setArray = Array.fill(aSize)(Set[T]())
    var current = 0
    def put(elem: T) = {
      if (setArray(current).size > sSize) {
        current = (current + 1) % aSize
        setArray(current).clear()
      }
      setArray(current).add(elem)
    }
    def contain(elem: T) = !setArray.par.forall { !_.contains(elem) }
    def almostUniq = iterator.map { elem =>
      if (contain(elem))
        None
      else {
        put(elem)
        Some(elem)
      }
    }.filter(_ != None).map(_.get)
  }
}
