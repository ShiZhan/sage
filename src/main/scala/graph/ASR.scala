package graph

/**
 * @author Zhan Shi
 * Aligned Sparse Row
 */
case class ASRFile(level: Int)(implicit prefix: String) {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import scala.collection.mutable.BitSet
  import ASR.{ V_SIZE, L_MAX, O_SCALE, O_MAX }
  require(level >= 0 && level < L_MAX)

  val N_SIZE = V_SIZE << level
  val N_MAX = 1 << level
  val L_BITS = level << O_SCALE

  val rm = new BitSet()
  def removed = rm.size

  val name = prefix + "%08x.asr".format(level)
  val p = Paths.get(name)
  val f = FileChannel.open(p, READ, WRITE, CREATE)
  val buf = ByteBuffer.allocate(N_SIZE).order(ByteOrder.LITTLE_ENDIAN)
  def close = f.close()

  def get(index: Int) = {
    require(index >= 0 && index < O_MAX)
    f.position(index << level)
    buf.clear()
    while (f.read(buf) != -1 && buf.hasRemaining()) {}
    buf.flip()
    Iterator.continually(buf.getLong).takeWhile(_ != -1)
  }

  def put(data: Iterator[Long]) = {
    buf.clear()
    data.take(N_MAX).foreach(buf.putLong)
    while (buf.hasRemaining()) buf.putLong(-1)
    buf.flip()
    val size = f.size
    val offset = size - 1
    f.position(size - 1)
    while (buf.hasRemaining()) f.write(buf)
    offset >> level
  }
  def remove(index: Int) = rm.add(index)
  def compress(idMap: Array[Int]) = {
    val tempFile = FileChannel.open(Paths.get("temp"), WRITE, CREATE)
    val size = f.size
    val last = (size - 1) >> level
    val blocks = (0 to last.toInt).toIterator.filterNot(rm.contains)
    for (b <- blocks) {
      val pos = b | L_BITS 
    }
    rm.clear()
    tempFile.close()
  }
}

class ASR(prefix: String) {
  import helper.Gauge.IteratorOperations
  import ASR.{ L_MAX, nextLevel }

  implicit val p = prefix
//  val degree = Map[Long, Int]()
//  val levels = Array.fill(L_MAX)(0)
  def put(edge: Edge) = {
    val Edge(u, v) = edge
//    val d = degree.getOrElse(u, 0)
//    val l = nextLevel(d)
//    if (l > 0) levels(l - 1) -= 1
//    levels(l) += 1
  }
  def putAll(edges: Iterator[Edge]) = edges.foreachDo(put)
  def get(vertex: Long) = { Iterator[Long]() }
}

object ASR {
  val V_SCALE = 3
  val L_SCALE = 5
  val O_SCALE = 32 - L_SCALE
  val V_SIZE = 1 << V_SCALE
  val L_MAX = 1 << L_SCALE
  val O_MAX = 1 << O_SCALE
  def nextLevel(d: Int) = (0 to L_MAX).find { l => (d >> l) == 0 } match {
    case Some(l) => l
    case _ => -1
  }
  def apply(prefix: String) = new ASR(prefix)
}