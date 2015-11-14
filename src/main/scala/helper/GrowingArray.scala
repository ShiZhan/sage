package helper

/**
 * @author ShiZhan
 * GrowingArray: Array grow as index been accessed
 */
class GrowingArray[T: Manifest](v: T) {
  import scala.collection.mutable.ArrayBuffer
  import GrowingArray.Const._

  val data = ArrayBuffer.fill(1)(Array.fill(rowSize)(v))
  private def more(n: Int) = for (i <- (1 to n)) data += Array.fill(rowSize)(v)
  val default = v

  def apply(index: Long) = {
    val high = (index >> rowScale).toInt
    val low = (index & rowMask).toInt
    val row = data.size
    if (high >= row) more(high - row + 1)
    data(high)(low)
  }

  def update(index: Long, d: T) = {
    val high = (index >> rowScale).toInt
    val low = (index & rowMask).toInt
    val row = data.size
    if (high >= row) more(high - row + 1)
    data(high)(low) = d
  }

  def size = data.size << rowScale
  def unVisited(index: Long) = this(index) == default
  def nUpdated = data.view.map { _.count { _ != default } }.sum
  def updated = {
    val id = Iterator.iterate(0L)(_ + 1)
    data.iterator.flatMap { _.iterator.map((id.next, _)) }.filterNot { _._2 == default }
  }
}

object GrowingArray {
  object Const {
    val rowScale = 20 // 0..19|20..44, index < (1 << 45)
    val rowSize = 1 << rowScale
    val rowMask = rowSize - 1L
  }

  def apply[T: Manifest](v: T) = new GrowingArray[T](v)
}
