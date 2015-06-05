/**
 * Huge Containers
 */
package helper

/**
 * @author ShiZhan
 * HugeArray:  Array indexed by Long
 * HugeBitSet: BitSet indexed by Long
 */
object HugeContainers {
  import scala.collection.mutable.ArrayBuffer
  import JStates.MEM

  private val scale = 21
  private val sizeB = 1 << scale
  private val maskB = sizeB - 1

  class GrowingArray[T](v: T) {
    val data = ArrayBuffer.fill(1, sizeB)(v)
    private def more(n: Int) = (1 to n).foreach { i =>
      val a = ArrayBuffer.fill(sizeB)(v)
      data.append(a)
    }
    def get(index: Long) = { // 0..21|22..43, index < (1 << 43)
      val high = (index >> scale).toInt
      if (high >= data.size) more(high - data.size + 1)
      val low = (index & maskB).toInt
      data(high)(low)
    }
    def put(index: Long, d: T) = {
      val high = (index >> scale).toInt
      if (high >= data.size) more(high - data.size + 1)
      val low = (index & maskB).toInt
      data(high)(low) = d
    }
    def size = data.size.toLong * sizeB.toLong
  }

  object GrowingArray {
    def apply[T](v: T) = new GrowingArray[T](v)
  }

  private val pow2s = (0 to 62).map(1L << _)
  private def nextPow2(l: Long) = pow2s.find(_ > l)
  private def prevPow2(l: Long) = pow2s.reverse.find(_ < l)
  private val maxSize = prevPow2(MEM.MAX) match { case Some(s) => s >> 3; case _ => -1 }

  class MaxArray[T: Manifest](v: T) {
    require(maxSize > 0 && maxSize < Int.MaxValue)
    val data = Array.fill(maxSize.toInt)(v)
    def get(index: Long) = data(index.toInt)
    def put(index: Long, d: T) = data(index.toInt) = d
    def unused(index: Long) = data(index.toInt) == v
    def used = (0 /: data) { (r, d) => if (d != v) r + 1 else r }
    def iterator = data.toIterator.zipWithIndex.filter(_._1 != v).map { case (d, i) => (i.toLong, d) }
    def size = data.length
  }

  object MaxArray {
    def apply[T: Manifest](v: T) = new MaxArray[T](v)
  }

  private val scaleB = 28
  private val scaleL = scaleB - 6
  private val sizeL = 1 << scaleL // 32 MB, 1 << 28 bits.
  private val maskL = (1 << 6) - 1
  def countBits(l: Long) = { var c = 0; var d = l; while (d != 0) { d &= (d - 1); c += 1 }; c }

  class LargeBitSet {
    val data = Array.fill(sizeL)(0L)
    def get(index: Long) = { (data((index >> 6).toInt) & (1 << (index & maskL))) != 0 }
    def set(index: Long) = { data((index >> 6).toInt) |= (1 << (index & maskL)) }
    def clear = { (0 to (sizeL - 1)).foreach { data.update(_, 0L) } }
    def size = (0 /: data) { (r, d) => r + countBits(d) }
    def isEmpty = data.forall(_ == 0)
  }

  object LargeBitSet {
    def apply() = new LargeBitSet()
  }
}