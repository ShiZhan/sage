package sage.test

object EdgeSortTest {
  import scala.util.Random
  import java.util.Scanner
  import graph.EdgeBase
  import helper.Timing._

  case class OrderedEdge(u: Long, v: Long) extends EdgeBase[OrderedEdge](u, v) with Ordered[OrderedEdge] {
    def compare(that: OrderedEdge) =
      if (u != that.u) {
        if ((u - that.u) > 0) 1 else -1
      } else if (v != that.v) {
        if ((v - that.v) > 0) 1 else -1
      } else 0
    def reverse = OrderedEdge(v, u)
  }

  def quickSort[T <: OrderedEdge](xs: Array[OrderedEdge]): Array[OrderedEdge] = {
    if (xs.length <= 1) xs
    else {
      Array.concat(
        quickSort(xs filter (xs.head > _)),
        xs filter (xs.head == _),
        quickSort(xs filter (xs.head < _)))
    }
  }

  def main(args: Array[String]) = {
    Random.setSeed(System.currentTimeMillis())
    println("input number of edges:")
    val sc = new Scanner(System.in)
    val n = sc.nextInt

    println("gnerating " + n + " random edges")
    val ls = Array.fill(n)(OrderedEdge(Random.nextInt(65536), Random.nextInt(65536)))

    val (r1, e1) = { () => quickSort(ls) }.elapsed
    println("quickSort time cost:  " + e1 + " ms")

    val (r2, e2) = { () => ls.sorted }.elapsed
    println("Seq.sorted time cost: " + e2 + " ms")
  }
}
