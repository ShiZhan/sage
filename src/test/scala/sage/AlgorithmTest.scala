package sage.test

object AlgorithmTest {
  import graph.{ Edge, SimpleEdge, WeightedEdge, Parallel, algorithms }
  import algorithms._
  import Parallel.{ Algorithm, Engine, Engine_W }
  import helper.GrowingArray
  import helper.Lines.LinesWrapper
  import helper.Timing._

  def main(args: Array[String]) =
    if (args.nonEmpty) {
      val engine = new Engine(args, "test.csv")
      val algorithms = Seq(
        new BFS_U(0),
        new SSSP_U(0),
        new CC(),
        new Degree_U(),
        new KCore(),
        new PageRank(10))

      engine.run(new PageRank(10))

      //for (a <- algorithms) {
      //  val (r, e) = { () => a.run }.elapsed
      //}
    } else println("run with <edge file(s)>")
}
