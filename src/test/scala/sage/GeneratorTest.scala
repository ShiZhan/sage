package sage.test

object GeneratorTest {
  import graph.generators._
  import helper.Timing._

  def main(args: Array[String]) = {
    val generators = Seq(
      ("RMAT   16 16     ", new RecursiveMAT(16, 16)),
      ("ER(S)  16 16     ", new ErdosRenyiSimplified(16, 16)),
      ("ER     12 16     ", new ErdosRenyi(12, 16)),
      ("SW     16 16 0.1 ", new SmallWorld(16, 16, 0.1)),
      ("BA(S)  16 16     ", new BarabasiAlbertSimplified(16, 16)),
      ("BA(OS) 16 16     ", new BarabasiAlbertOverSimplified(16, 16)),
      ("BA     10 16     ", new BarabasiAlbert(10, 16)))

    println("                 %12s%12s%12s".format("edges", "time (ms)", "speed (K/s)"))
    for ((description, generator) <- generators) {
      val edges = generator.getEdges
      val (nEdge, elapsed) = { () => (0 /: edges) { (r, e) => r + 1 } }.elapsed
      val speed = nEdge / elapsed
      val result = "%12d%12d%12d ".format(nEdge, elapsed, speed)
      println(s"$description$result")
    }
  }
}
