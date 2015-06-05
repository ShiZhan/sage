package sage.test

object MemSequentialRandomTest {
  import scala.util.Random
  import graph.Edge
  import helper.Timing._

  val edgeTotal = 1 << 23

  def main(args: Array[String]) = {
    println(s"prepare $edgeTotal edges")
    val edgeArray = Array.fill(edgeTotal)(Edge(Random.nextInt, Random.nextInt))
    println("prepare sequential and random addresses")
    val sAddress = (0 to (edgeTotal - 1)).toArray
    val rAddress = Array.fill(edgeTotal)(Random.nextInt(edgeTotal))
    println("comparing...")
    var counter = 0
    val esr = { () =>
      (1 to edgeTotal).foreach { i => val Edge(u, v) = edgeArray(sAddress(i - 1)); counter += 1 }
    }.elapsed
    println(s"$counter sequential read,  $esr ms")
    counter = 0
    val err = { () =>
      (1 to edgeTotal).foreach { i => val Edge(u, v) = edgeArray(rAddress(i - 1)); counter += 1 }
    }.elapsed
    println(s"$counter random     read,  $err ms")
    counter = 0
    val esw = { () =>
      (1 to edgeTotal).foreach { i => edgeArray(sAddress(i - 1)) = Edge(-1, -1); counter += 1 }
    }.elapsed
    println(s"$counter sequential write, $esw ms")
    counter = 0
    val erw = { () =>
      (1 to edgeTotal).foreach { i => edgeArray(rAddress(i - 1)) = Edge(-1, -1); counter += 1 }
    }.elapsed
    println(s"$counter random     write, $erw ms")
  }
}
