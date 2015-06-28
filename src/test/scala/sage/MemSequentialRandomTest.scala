package sage.test

object MemSequentialRandomTest {
  import scala.util.Random
  import graph.Edge
  import helper.Timing._

  val edgeTotal = 1 << 23
  val edgeRange = 0 to (edgeTotal - 1)

  def main(args: Array[String]) = {
    println(s"prepare $edgeTotal edges")
    val edgeArray = Array.fill(edgeTotal)(Edge(Random.nextInt, Random.nextInt))
    println("prepare sequential and random addresses")
    val sAddress = edgeRange.toArray
    val rAddress = Array.fill(edgeTotal)(Random.nextInt(edgeTotal))
    println("comparing...")

    val esr = { () => edgeRange.foreach { i => val Edge(u, v) = edgeArray(sAddress(i)) } }.elapsed
    println("%8d sequential  read, %8d ms, % 8d MB/s".format(edgeTotal, esr, (edgeTotal >> 16) * 1000 / esr))

    val err = { () => edgeRange.foreach { i => val Edge(u, v) = edgeArray(rAddress(i)) } }.elapsed
    println("%8d random      read, %8d ms, % 8d MB/s".format(edgeTotal, err, (edgeTotal >> 16) * 1000 / err))

    val esw = { () => edgeRange.foreach { i => edgeArray(sAddress(i)) = Edge(-1, -1) } }.elapsed
    println("%8d sequential write, %8d ms, % 8d MB/s".format(edgeTotal, esw, (edgeTotal >> 16) * 1000 / esw))

    val erw = { () => edgeRange.foreach { i => edgeArray(rAddress(i)) = Edge(-1, -1) } }.elapsed
    println("%8d random     write, %8d ms, % 8d MB/s".format(edgeTotal, erw, (edgeTotal >> 16) * 1000 / erw))
  }
}
