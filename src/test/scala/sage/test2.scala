package miscs

case class Point(x: Int, y: Int) extends Ordered[Point] {
  override def compare(that: Point) = (if (x != that.x) (x - that.x) else (y - that.y))
  def ==(that: Point) = compare(that) == 0
}

object cmp {
  import scala.util.Random
  import java.util.Scanner
  import helper.Timing._

  def main(args: Array[String]) {
    Random.setSeed(System.currentTimeMillis())
    val sc = new Scanner(System.in)
    val n = sc.nextInt
    val ls = Array.fill(n)(new Point(Random.nextInt(65536), Random.nextInt(65536)))

    val (r1, e1) = { () => quickSort(ls) }.elapsed
    println("quickSort time cost: " + e1 + " ms")

    val (r2, e2) = { () => ls.sorted }.elapsed
    println("Seq.sorted time cost: " + e2 + " ms")
  }

  def quickSort[T <: Point](xs: Array[Point]): Array[Point] = {
    if (xs.length <= 1) xs
    else {
      Array.concat(
        quickSort(xs filter (xs.head > _)),
        xs filter (xs.head == _),
        quickSort(xs filter (xs.head < _)))
    }
  }
}