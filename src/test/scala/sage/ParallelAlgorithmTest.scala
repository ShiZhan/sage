package sage.test

object ParallelAlgorithmTest {
  import graph.{ SimpleEdge, EdgeProvider }
  import generators.RecursiveMAT
  import algorithms.{ BFS_U, Degree_U, CC, KCore, PageRank }
  import algorithms.parallel.{ BFS_U => BFS_UP, Degree_U => Degree_UP, CC => CC_P, KCore => KCore_P, PageRank => PageRank_P }
  import helper.Timing._

  class TestEdgeProvider(edgeArray: Array[SimpleEdge]) extends EdgeProvider[SimpleEdge] {
    def getEdges = edgeArray.toIterator
  }

  def main(args: Array[String]) = {
    val edges = new RecursiveMAT(18, 8).getEdges.toArray
    implicit val edgeProvider = new TestEdgeProvider(edges)
    implicit val edgeProviders = edges.grouped(1 << 18).map(new TestEdgeProvider(_)).toSeq
    val nGroups = 1 << (18 + 3 - 18)

    val (result0, e0) = { () => new Degree_U().run }.elapsed
    println(s"Degree 1 group: $e0 ms")
    val (result1, e1) = { () => new Degree_UP().run }.elapsed
    println(s"Degree $nGroups groups: $e1 ms")

    val (result2, e2) = { () => new BFS_U(0).run }.elapsed
    println(s"BFS 1 group: $e2 ms")
    val (result3, e3) = { () => new BFS_UP(0).run }.elapsed
    println(s"BFS $nGroups groups: $e3 ms")

    val (result4, e4) = { () => new CC().run }.elapsed
    println(s"CC 1 group: $e4 ms")
    val (result5, e5) = { () => new CC_P().run }.elapsed
    println(s"CC $nGroups groups: $e5 ms")

    val (result6, e6) = { () => new KCore().run }.elapsed
    println(s"KCore 1 group: $e6 ms")
    val (result7, e7) = { () => new KCore_P().run }.elapsed
    println(s"KCore $nGroups groups: $e7 ms")

    val (result8, e8) = { () => new PageRank(10).run }.elapsed
    println(s"PageRank 1 group: $e8 ms")
    val (result9, e9) = { () => new PageRank_P(10).run }.elapsed
    println(s"PageRank $nGroups groups: $e9 ms")
  }
}
