package sage.test

object FileBufAccessTest {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import graph.Edge
  import graph.Edges.edgeScale
  import helper.Timing._
  import helper.IteratorOps.ClosableIteratorWrapper

  def edges = {
    var i = -1L
    Iterator.continually { i += 1; Edge(i, i + 1) }.take(1 << 22)
  }

  def bufferedReadWrite(fc: FileChannel, scale: Int) = {
    val size = 1 << scale
    val gSize = 1 << (scale - edgeScale)
    val buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)
    val ew = {
      () =>
        {
          fc.position(0)
          edges.grouped(gSize).foreach { g =>
            buf.clear()
            for (Edge(u, v) <- g) { buf.putLong(u); buf.putLong(v) }
            buf.flip()
            while (buf.hasRemaining) fc.write(buf)
          }
        }
    }.elapsed
    println("%9d buffer write: %9d ms".format(size, ew))
    val length = fc.size()
    fc.position(0)
    val er = {
      () =>
        {
          while (fc.position < length) {
            buf.clear()
            while (fc.read(buf) != -1 && buf.hasRemaining) {}
            buf.flip()
            while (buf.hasRemaining) {
              val u = buf.getLong
              val v = buf.getLong
              //Edge(u, v)
            }
          }
        }
    }.elapsed
    println("%9d buffer read:  %9d ms".format(size, er))
  }

  def show(fc: FileChannel) = {
    val buf = ByteBuffer.allocate(1 << 17).order(ByteOrder.LITTLE_ENDIAN)
    fc.position(0)
    Iterator.continually {
      buf.clear()
      while (fc.read(buf) != -1 && buf.hasRemaining) {}
      buf.flip()
      buf
    }.takeWhile(_ => buf.hasRemaining).flatMap { b =>
      val nEdge = b.remaining() >> edgeScale
      Iterator.continually { Edge(b.getLong, b.getLong) }.take(nEdge)
    }.atLast(println).foreach { e => print(e + "\r") }
  }

  def main(args: Array[String]) = {
    val name = "test.bin"
    val p = Paths.get(name)
    val fc = FileChannel.open(p, READ, WRITE, CREATE)
    (edgeScale to 20).foreach { bufferedReadWrite(fc, _) }
    show(fc)
    fc.close()
  }
}
