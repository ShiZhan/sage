package helper

/**
 * @author ShiZhan
 * GrowingArray:  Array grow as index been accessed
 */
class GrowingArray[T: Manifest](v: T) {
  import scala.collection.mutable.ArrayBuffer
  import GrowingArray.Const._

  val data = ArrayBuffer.fill(1)(Array.fill(sizeRow)(v))
  private def more(n: Int) = (1 to n).foreach { i => data += Array.fill(sizeRow)(v) }
  val default = v

  def apply(index: Int) = { // 0..20|21..30, index < (1 << 30)
    val row = data.size
    val high = index >> scaleRow
    if (high >= row) more(high - row + 1)
    val low = index & maskRow
    data(high)(low)
  }

  def update(index: Long, d: T) = {
    val row = data.size
    val high = (index >> scaleRow).toInt
    if (high >= row) more(high - row + 1)
    val low = (index & maskRow).toInt
    data(high)(low) = d
  }

  def size = data.size.toLong << scaleRow
  def unused(index: Int) = this(index) == default
  def used = (0L /: data.toIterator.flatten.filter(_ != default)) { (r, d) => r + 1 }
  def inUse = {
    val i = Iterator.from(0)
    data.toIterator.flatten.map { d => (i.next, d) }.filter { _._2 != default }
  }
}

object GrowingArray {
  object Const {
    val scaleRow = 20
    val sizeRow = 1 << scaleRow
    val maskRow = sizeRow - 1
  }

  def apply[T: Manifest](v: T) = new GrowingArray[T](v)
}
