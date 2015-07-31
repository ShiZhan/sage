package sage.test

object AlgorithmTest {
  import graph.{ Edge, SimpleEdge, WeightedEdge, ParallelEngine, algorithms }
  import algorithms._
  import ParallelEngine.{ Algorithm, Engine, Engine_W }
  import helper.GrowingArray
  import helper.Lines.LinesWrapper
  import helper.Timing._

  def main(args: Array[String]) =
    if (args.nonEmpty) {
      val engine = new Engine(args)
      //val engine_w = new Engine_W(args)
      //val algorithms = Seq(
      //  new BFS(0), new BFS_U(0),
      //  new SSSP(0), new SSSP_U(0),
      //  new CC(),
      //  new Degree(), new Degree_U(),
      //  new KCore(),
      //  new PageRank(10))

      engine.run(new PageRank(10))

      //for (a <- algorithms) {
      //  val (r, e) = { () => a.run }.elapsed
      //}
    } else println("run with <edge file(s)>")
}
