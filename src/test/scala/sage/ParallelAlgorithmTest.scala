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
    val edges = new RecursiveMAT(12, 8).getEdges.filterNot(_.selfloop).toArray
    implicit val edgeProvider = new TestEdgeProvider(edges)
    implicit val edgeProviders = edges.grouped(1 << 12).map(new TestEdgeProvider(_)).toSeq
    val nGroups = 1 << (12 + 3 - 12)

    val a0s = new Degree_U()
    val a0p = new Degree_UP()
    val a1s = new BFS_U(0)
    val a1p = new BFS_UP(0)
    val a2s = new CC()
    val a2p = new CC_P()
    val a3s = new KCore()
    val a3p = new KCore_P()
    val a4s = new PageRank(10)
    val a4p = new PageRank_P(10)

    val (r0s, e0s) = { () => a0s.run }.elapsed
    println(s"Degree 1 group: $e0s ms")
    val (r0p, e0p) = { () => a0p.run }.elapsed
    println(s"Degree $nGroups groups: $e0p ms, ${a0s.vertices == a0p.vertices}")

    val (r1s, e1s) = { () => a1s.run }.elapsed
    println(s"BFS 1 group: $e1s ms")
    val (r1p, e1p) = { () => a1p.run }.elapsed
    println(s"BFS $nGroups groups: $e1p ms, ${a1s.vertices == a1p.vertices}")

    val (r2s, e2s) = { () => a2s.run }.elapsed
    println(s"CC 1 group: $e2s ms")
    val (r2p, e2p) = { () => a2p.run }.elapsed
    println(s"CC $nGroups groups: $e2p ms, ${a2s.vertices == a2p.vertices}")

    val (r3s, e3s) = { () => a3s.run }.elapsed
    println(s"KCore 1 group: $e3s ms")
    val (r3p, e3p) = { () => a3p.run }.elapsed
    println(s"KCore $nGroups groups: $e3p ms, ${a3s.vertices == a3p.vertices}")
    println((a3s.vertices <> a3p.vertices).size)

    val (r4s, e4s) = { () => a4s.run }.elapsed
    println(s"PageRank 1 group: $e4s ms")
    val (r4p, e4p) = { () => a4p.run }.elapsed
    println(s"PageRank $nGroups groups: $e4p ms")
  }
}
