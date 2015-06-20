package sage.test

object AlgorithmTest {
  import graph.{ Edge, WEdge, EdgeProvider }
  import generators.RecursiveMAT
  import graph.EdgeUtils.WEdgeConverter
  import algorithms._
  import helper.Timing._

  class TestEdgeProvider extends EdgeProvider[Edge] {
    val edges = new RecursiveMAT(8, 8).getEdges.toArray
    def getEdges = edges.toIterator
  }

  class TestWeightedEdgeProvider extends EdgeProvider[WEdge] {
    val edges = new RecursiveMAT(8, 8).getEdges.toWEdges.toArray
    def getEdges = edges.toIterator
  }

  def main(args: Array[String]) = {
    implicit lazy val edgeProvider = new TestEdgeProvider
    implicit lazy val wEdgeProvider = new TestWeightedEdgeProvider
    val (result0, e0) = { () => new BFS(0).run }.elapsed
    val (result1, e1) = { () => new BFS_U(0).run }.elapsed
    val (result2, e2) = { () => new SSSP(0).run }.elapsed
    val (result3, e3) = { () => new SSSP_U(0).run }.elapsed
    val (result4, e4) = { () => new CC().run }.elapsed
    val (result5, e5) = { () => new Degree().run }.elapsed
    val (result6, e6) = { () => new Degree_U().run }.elapsed
    val (result7, e7) = { () => new KCore().run }.elapsed
    val (result8, e8) = { () => new PageRank(10).run }.elapsed
  }
}
