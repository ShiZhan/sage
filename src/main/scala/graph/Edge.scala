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

  implicit class EdgesWriter(edges: Iterator[Edge]) {
    def toFile(edgeFile: String) = Lines.toFile(edges.map(_.toString), edgeFile)
  }
}
