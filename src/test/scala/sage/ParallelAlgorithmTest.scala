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

    val algorithms = Seq(
      (new Degree_U(), new Degree_UP()),
      (new BFS_U(0), new BFS_UP(0)),
      (new CC(), new CC_P()),
      (new KCore(), new KCore_P()),
      (new PageRank(10), new PageRank_P(10)))

    for ((s, p) <- algorithms) {
      val (rs, es) = { () => s.run }.elapsed
      println(s"${s.getClass.getName} 1 group: $es ms")
      val (rp, ep) = { () => p.run }.elapsed
      println(s"${p.getClass.getName} $nGroups groups: $ep ms")
      val r0 = rs.toMap
      val r1 = rp.toMap
      println(s"diff ${r0.filterNot { case (k, v) => r1(k) == v }.size}")
    }
  }
}
