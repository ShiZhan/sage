package sage.test

object SequentialRateImpact {
  import helper.Timing._

  val N = 1 << 25

  def main(args: Array[String]) = {
    println(s"prepare $N elements")
    val items = Array.tabulate(N) { i => (i + 1) % N }

    val cases = Seq(0, 4, 8, 12, 16, 20, 24).map { s => (1 << s, N >> s) }
    println("     " + cases.map(_._1).map("%9d".format(_)).mkString + " (ms)")
    print("MEM  ")
    for ((g, n) <- cases) {
      for (i <- Iterator.from(0).take(g)) {
        val u = (i + 1) * n - 1
        val v = ((i - 1 + g) % g) * n
        items(u) = v
      }
      def op = () => { var i = 1; while (i != 0) i = items(i) }
      val e = { op }.elapsed
      print("%9d".format(e))
    }
    println
  }
}
