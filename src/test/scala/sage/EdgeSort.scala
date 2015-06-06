package sage.test.Edge

object EdgeSort {
  import scala.util.Random
  import java.util.Scanner
  import graph.Edge
  import helper.Timing._

  def quickSort[T <: Edge](xs: Array[Edge]): Array[Edge] = {
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

    val sc = new Scanner(System.in)
    val n = sc.nextInt
    println(n + " random edges")
    val ls = Array.fill(n)(Edge(Random.nextInt(65536), Random.nextInt(65536)))

    val (r1, e1) = { () => quickSort(ls) }.elapsed
    println("quickSort time cost:  " + e1 + " ms")

    val (r2, e2) = { () => ls.sorted }.elapsed
    println("Seq.sorted time cost: " + e2 + " ms")
  }
}
