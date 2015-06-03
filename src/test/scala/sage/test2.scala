package miscs

/*
 * test case for quick sort and timing wrapper functions
 */
object EdgeSort {
  import scala.util.Random
  import java.util.Scanner
  import graph.Edge
  import helper.Timing._

  def quickSort[T <: Edge](xs: Array[Edge]): Array[Edge] = {
    if (xs.length <= 1) xs
    else {
      Array.concat(
        quickSort(xs filter (xs.head > _)),
        xs filter (xs.head == _),
        quickSort(xs filter (xs.head < _)))
    }
  }

  def main(args: Array[String]) = {
    Random.setSeed(System.currentTimeMillis())

    val sc = new Scanner(System.in)
    val n = sc.nextInt
    println(n + " random edges")
    val ls = Array.fill(n)(Edge(Random.nextInt(65536), Random.nextInt(65536)))

    val (r1, e1) = { () => quickSort(ls) }.elapsed
    println("quickSort time cost:  " + e1 + " ms")

    val (r2, e2) = { () => ls.sorted }.elapsed
    println("Seq.sorted time cost: " + e2 + " ms")
  }
}

object FileBufAccessTest {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import graph.Edge
  import graph.Edges.edgeScale
  import helper.Timing._

  def edges = {
    var i = -1L
    Iterator.continually { i += 1; Edge(i, i + 1) }.take(1 << 20)
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
    }.foreach { e => print(e + "\r") }
  }

  def main(args: Array[String]) = {
    val name = "test.bin"
    val p = Paths.get(name)
    val fc = FileChannel.open(p, READ, WRITE, CREATE)
    (edgeScale to 17).foreach { bufferedReadWrite(fc, _) }
    show(fc)
    fc.close()
  }
}