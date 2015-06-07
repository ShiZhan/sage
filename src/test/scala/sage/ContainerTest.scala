package sage.test.HugeContainers

object GrowingArrayTest {
  import helper.HugeContainers.{ GrowingArray, MaxSize, HugeArrayOps }
  import helper.Timing._

  def main(args: Array[String]) = {
    val ha = GrowingArray[Long](0L)
    val sizeB = 1 << 20
    val maskB = sizeB - 1
    println("-------------------")
    (0 to maskB).foreach { i => ha(i) = i }
    val head0 = ha(0); val tail0 = ha(maskB)
    val size0 = ha.size
    println(s"1st [0 .. $maskB]: $head0 .. $tail0, $size0")
    println("-------------------")
    (0 to maskB).map(_ + sizeB).foreach { i => ha(i) = i }
    val head1 = ha(sizeB); val tail1 = ha(sizeB + maskB)
    val size1 = ha.size
    println(s"2nd [0 .. $maskB]: $head1 .. $tail1, $size1")
    val expected = MaxSize.forLong.toInt + 1
    val e = { () => (0 to (expected - 1)).reverse.foreach { i => ha(i) = expected - i } }.elapsed
    val size = ha.size
    println("-------------------")
    println(s"expected $expected, allocated $size elements, $e ms")
    println("element (0): " + ha(0))
    println("element (1): " + ha(1))
    println("element (2): " + ha(2))
    println("element (" + (expected - 1) + "): " + ha(expected - 1))
    println("element (" + (size - 3) + "): " + ha(size - 3))
    println("element (" + (size - 2) + "): " + ha(size - 2))
    println("element (" + (size - 1) + "): " + ha(size - 1))
    println("used: " + ha.used)
  }
}

object MaxArrayTest {
  import helper.HugeContainers.{ MaxArray, HugeArrayOps }
  import helper.Timing._

  def main(args: Array[String]) = {
    val ma = MaxArray[Long](-1L)
    val size = ma.size.toInt
    val e = { () => (0 to (size - 1)).reverse.foreach { i => ma(i) = size - i } }.elapsed
    println(s"$size elements, $e ms")
    println("element (0): " + ma(0))
    println("element (1): " + ma(1))
    println("element (2): " + ma(2))
    println("element (" + (size - 3) + "): " + ma(size - 3))
    println("element (" + (size - 2) + "): " + ma(size - 2))
    println("element (" + (size - 1) + "): " + ma(size - 1))
    println("used: " + ma.used)
  }
}

object FlatArrayTest {
  import helper.HugeContainers.{ FlatArray, MaxSize, HugeArrayOps }
  import helper.Timing._

  def main(args: Array[String]) = {
    val expected = MaxSize.forLong.toInt + 1
    val fa = FlatArray[Long](expected, -1L)
    val e = { () => (0 to (expected - 1)).reverse.foreach { i => fa(i) = expected - i } }.elapsed
    val size = fa.size
    println(s"expected $expected, allocated $size elements, $e ms")
    println("element (0): " + fa(0))
    println("element (1): " + fa(1))
    println("element (2): " + fa(2))
    println("element (" + (expected - 1) + "): " + fa(expected - 1))
    println("element (" + (size - 3) + "): " + fa(size - 3))
    println("element (" + (size - 2) + "): " + fa(size - 2))
    println("element (" + (size - 1) + "): " + fa(size - 1))
    println("used: " + fa.used)
  }
}