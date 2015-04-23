package graph

import java.nio.{ ByteBuffer, ByteOrder }

case class Edge(u: Long, v: Long) {
  def toBytes =
    ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(u).putLong(v).array()

  override def toString = s"$u $v"
}

object EdgeConverters {
  implicit class Bytes2Edge(bytes: Array[Byte]) {
    private val buf =
      if (bytes.length == 16)
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      else
        ByteBuffer.wrap(Array.fill(16)(Byte.MaxValue))

    def toEdge = Edge(buf.getLong, buf.getLong)
  }
}