package helper

/**
 * @author ShiZhan
 * GrowingArray:  Array grow as index been accessed
 */
class GrowingArray[T: Manifest](v: T) {
  import scala.collection.mutable.ArrayBuffer
  import GrowingArray.Const._

  val data = ArrayBuffer.fill(1)(Array.fill(rowSize)(v))
  private def more(n: Int) = (1 to n).foreach { i => data += Array.fill(rowSize)(v) }
  val default = v

  def apply(index: Int) = {
    val high = index >> rowScale
    val low = index & rowMask
    val row = data.size
    if (high >= row) more(high - row + 1)
    data(high)(low)
  }

  def update(index: Int, d: T) = {
    val high = index >> rowScale
    val low = index & rowMask
    val row = data.size
    if (high >= row) more(high - row + 1)
    data(high)(low) = d
  }

  def size = data.size << rowScale
  def unVisited(index: Int) = this(index) == default
  def nUpdated = data.view.map { _.count { _ != default } }.sum
  def updated = {
    val i = Iterator.from(0)
    data.iterator.flatMap { _.iterator.map((i.next, _)) }.filterNot { _._2 == default }
  }
}

object GrowingArray {
  object Const {
    val rowScale = 20 // 0..20|21..30, index < (1 << 30)
    val rowSize = 1 << rowScale
    val rowMask = rowSize - 1
  }

  def apply[T: Manifest](v: T) = new GrowingArray[T](v)
}
