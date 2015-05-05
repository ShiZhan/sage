package graph

import java.nio.{ ByteBuffer, ByteOrder }

case class Edge(u: Long, v: Long) {
  def valid = (u & v) != Long.MaxValue

  def toBytes =
    ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(u).putLong(v).array()

  override def toString = s"$u $v"
}

object EdgeUtils extends helper.Logging {
  val invalidEdge = Edge(Long.MaxValue, Long.MaxValue)

  implicit class Bytes2Edge(bytes: Array[Byte]) {
    require(bytes.length == 16)

    private val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    def toEdge = Edge(buf.getLong, buf.getLong)
  }

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); invalidEdge
    case from :: to :: Nil =>
      Edge(from.toLong, to.toLong)
    case _ =>
      logger.error("invalid: [{}]", line); invalidEdge
  }

  def fromFile(edgeFile: String) =
    helper.GetLines.fromFileOrConsole(edgeFile).map(line2edge).filter(_.valid)
}
