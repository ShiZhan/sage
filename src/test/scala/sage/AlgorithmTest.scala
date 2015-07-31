//package sage.test
//
//object AlgorithmTest {
//  import graph.{ SimpleEdge, WeightedEdge, EdgeProvider }
//  import generators.RecursiveMAT
//  import graph.EdgeUtils.edge2wedge
//  import algorithms._
//  import helper.Timing._
//
//  class TestEdgeProvider extends EdgeProvider[SimpleEdge] {
//    val edges = new RecursiveMAT(8, 8).getEdges.toArray
//    def getEdges = edges.toIterator
//  }
//
//  class TestWeightedEdgeProvider extends EdgeProvider[WeightedEdge] {
//    val edges = new RecursiveMAT(8, 8).getEdges.map(edge2wedge).toArray
//    def getEdges = edges.toIterator
//  }
//
//  def main(args: Array[String]) = {
//    implicit lazy val edgeProvider = new TestEdgeProvider
//    implicit lazy val wEdgeProvider = new TestWeightedEdgeProvider
//    val algorithms = Seq(
//      new BFS(0), new BFS_U(0),
//      new SSSP(0), new SSSP_U(0),
//      new CC(),
//      new Degree(), new Degree_U(),
//      new KCore(),
//      new PageRank(10))
//    for (a <- algorithms) {
//      val (r, e) = { () => a.run }.elapsed
//    }
//  }
//}
//package sage.test
object AlgorithmTest {
  import graph.{ Edge, SimpleEdge, WeightedEdge, ParallelEngine, algorithms }
  import algorithms._
  import ParallelEngine.{ Algorithm, Engine, Engine_W }
  import helper.GrowingArray
  import helper.Lines.LinesWrapper
  import helper.Timing._

  def main(args: Array[String]) =
    if (args.nonEmpty) {
      new Engine_W(args).run(new SSSP_U(0))
    } else println("run with <edge file(s)>")
}
