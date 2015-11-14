package helper

/**
 * @author ShiZhan
 * GrowingBitSet: BitSet grow as index been accessed
 */
class GrowingBitSet() {
  import scala.collection.mutable.{ ArrayBuffer, BitSet }
  import GrowingBitSet.Const._

  val data = ArrayBuffer.fill(1)(new BitSet(rowSize))
  private def more(n: Int) = for (i <- (1 to n)) data += new BitSet(rowSize)

  def apply(index: Long) = {
    val high = (index >> rowScale).toInt
    val low = (index & rowMask).toInt
    val row = data.size
    if (high >= row) more(high - row + 1)
    data(high)(low)
  }

  def add(index: Long) = {
    val high = (index >> rowScale).toInt
    val low = (index & rowMask).toInt
    val row = data.size
    if (high >= row) more(high - row + 1)
    data(high).add(low)
  }

  def foreach(f: Long => Unit) = for (
    (row, id) <- data.view.zipWithIndex;
    high = id.toLong << rowScale;
    elem <- row;
    low = elem.toLong
  ) f(low + high)

  def iterator() = data.iterator.zipWithIndex.flatMap {
    case (row, id) =>
      val high = id.toLong << rowScale
      row.iterator.map(_.toLong + high)
  }

  def size = (0L /: data) { _ + _.size }
  def nonEmpty() = data.exists { _.nonEmpty }
  def clear() = for (row <- data) row.clear()
}

object GrowingBitSet {
  object Const {
    val rowScale = 23 // 0..22|23..44, index < (1 << 45)
    val rowSize = 1 << rowScale // 1MB per row
    val rowMask = rowSize - 1L
  }

  def apply() = new GrowingBitSet()
}
