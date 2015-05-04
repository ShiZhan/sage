/**
 * Bloom Filter for removing duplicate edges
 */
package helper

import scala.math._

class BitArray(bits: Int) {
  val size = nextPow2(bits)
  require(isPowerOf2(size))
  require(size >= 64)
  val data = new Array[Long](size >> 6)

  def set(index: Int): Unit = data(idx(index)) |= (1L << index)
  def get(index: Int): Boolean = (data(idx(index)) & (1L << index)) != 0
  def clear = (0 to ((size >> 6) - 1)).foreach(data.update(_, 0L))

  private val mask = size - 1
  private def idx(index: Int) = (index & mask) >> 6
  private def isPowerOf2(i: Int) = ((i - 1) & i) == 0
  private def nextPow2(i: Int) = {
    def highestBit(remainder: Int, c: Int): Int =
      if (remainder > 0) highestBit(remainder >> 1, c + 1) else c
    require(i <= (1 << 30))
    val n = if (isPowerOf2(i)) i else 1 << highestBit(i, 0)
    assert(n >= i && i * 2 > n && isPowerOf2(n))
    n
  }
}

/**
 * An implementation of the [Bloom Filter algorithm](http://en.wikipedia.org/wiki/Bloom_filter)
 * references:
 * 1. https://github.com/birchsport/scala-bloomfilter
 * 2. http://theyougen.blogspot.com/2009/12/decent-bloom-filter-in-scala.html
 */
class BloomFilter(val size: Int, val expectedElements: Int) {
  require(size > 0)
  require(expectedElements > 0)

  val bitArray = new BitArray(size)
  val k = ceil((bitArray.size / expectedElements) * log(2.0)).toInt
  val expectedFalsePositiveProbability = pow(1 - exp(-k * 1.0 * expectedElements / bitArray.size), k)

  /**
   * Add any object to the filter.
   * Will invoke the 'hashCode' method on the passed in object
   */
  def add(any: Any) {
    add(any.hashCode)
  }

  /**
   * Add a hash value to the filter
   */
  def add(hash: Int) {
    def add(i: Int, seed: Int) {
      if (i == k) return
      val next = xorRandom(seed)
      bitArray.set(next)
      add(i + 1, next)
    }
    add(0, hash)
  }

  /**
   * Checks to see if any object is in the filter.
   * Will invoke the 'hashCode' method on the passed in object
   */
  def contains(any: Any): Boolean = {
    contains(any.hashCode)
  }

  /**
   * Checks to see if a given hash value is in the filter
   */
  def contains(hash: Int): Boolean = {
    def contains(i: Int, seed: Int): Boolean = {
      if (i == k) return true
      val next = xorRandom(seed)
      if (!bitArray.get(next)) return false
      return contains(i + 1, next)
    }
    contains(0, hash)
  }

  private def xorRandom(i: Int) = {
    var y = i
    y ^= y << 13
    y ^= y >> 17
    y ^ y << 5
  }

}
