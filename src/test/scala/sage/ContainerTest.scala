package sage.test.HugeContainers

object GrowingArrayTest {
  import helper.HugeContainers.{ GrowingArray, MaxSize }
  import helper.Timing._

  def main(args: Array[String]) = {
    val ha = GrowingArray[Long](0L)
    val sizeB = 1 << 21
    val maskB = sizeB - 1
    println(s"---\nfirst [0 .. $maskB]")
    (0 to maskB).foreach { i => ha(i, i) }
    val head0 = ha(0); val tail0 = ha(maskB)
    val size0 = ha.size
    println(s"$head0 .. $tail0, $size0")
    println(s"---\nnext  [0 .. $maskB]")
    (0 to maskB).map(_ + sizeB).foreach { i => ha(i, i) }
    val head1 = ha(sizeB); val tail1 = ha(sizeB + maskB)
    val size1 = ha.size
    println(s"$head1 .. $tail1, $size1")
    val total = MaxSize.forLong.toInt
    val e = { () => (0 to (total - 1)).reverse.foreach { i => ha(i, total - i) } }.elapsed
    val size2 = ha.size
    println(s"---\n$size2 elements ...")
    val head2 = ha(0L); val tail2 = ha(total - 1)
    println(s"$head2 .. $tail2")
    println(s"$e ms")
  }
}

object MaxArrayTest {
  import helper.HugeContainers.MaxArray
  import helper.Timing._

  def main(args: Array[String]) = {
    val ma = MaxArray[Long](-1L)
    val total = ma.size
    val e = { () => (0 to (total - 1)).reverse.foreach { i => ma.put(i, total - i) } }.elapsed
    println(s"$total elements, $e ms")
  }
}

object FlatArrayTest {
  import helper.HugeContainers.{ FlatArray, MaxSize }
  import helper.Timing._

  def main(args: Array[String]) = {
    val total = MaxSize.forLong.toInt + 1
    val fa = FlatArray[Long](total, -1L)
    val e = { () => (0 to (total - 1)).reverse.foreach { i => fa(i, total - i) } }.elapsed
    println(s"expected $total elements, $e ms")
  }
}