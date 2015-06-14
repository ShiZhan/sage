package sage.test

object AlgorithmTest {
  import graph.EdgeProvider
  import generators.RecursiveMAT
  import algorithms._
  import helper.Timing._

  class TestEdgeProvider extends EdgeProvider {
    val edges = new RecursiveMAT(8, 8).getEdges.toArray
    def getEdges = edges.toIterator
  }

  def main(args: Array[String]) = {
    implicit val edgeProvider = new TestEdgeProvider
    val (result0, e0) = { () => new BFS(0).run }.elapsed
    val (result1, e1) = { () => new BFS_U(0).run }.elapsed
    val (result2, e2) = { () => new SSSP(0).run }.elapsed
    val (result3, e3) = { () => new CC().run }.elapsed
    val (result4, e4) = { () => new Degree().run }.elapsed
    val (result5, e5) = { () => new Degree_U().run }.elapsed
//    val (result6, e6) = { () => new KCore().run }.elapsed
  }
}
