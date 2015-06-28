package sage.test

object ParallelBFSTest {
  import graph.{ SimpleEdge, EdgeProvider }
  import generators.RecursiveMAT
  import algorithms._
  import helper.Timing._

  class TestEdgeProvider(edgeArray: Array[SimpleEdge]) extends EdgeProvider[SimpleEdge] {
    def getEdges = edgeArray.toIterator
  }

  def main(args: Array[String]) = {
    val edges = new RecursiveMAT(20, 8).getEdges.toArray
    implicit lazy val edgeProvider = new TestEdgeProvider(edges)
    implicit lazy val edgeProviders = edges.grouped(1 << 18).map(new TestEdgeProvider(_)).toSeq
    val nGroups = 1 << (20 + 3 - 18)

    val (result0, e0) = { () => new BFS_U(0).run }.elapsed
    println(s"BFS 1 group: $e0 ms")
    val (result1, e1) = { () => new BFS_UP(0).run }.elapsed
    println(s"BFS $nGroups groups: $e1 ms")
  }
}
