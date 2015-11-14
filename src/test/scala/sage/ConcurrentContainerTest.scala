package sage.test

object ConcurrentContainerTest {
  import scala.collection.mutable.Map
  import scala.collection.concurrent.TrieMap
  import graph.{ Edge, SimpleEdge, EdgeProvider }
  import graph.generators.RecursiveMAT
  import helper.GrowingArray
  import helper.Timing._

  class TestEdgeProvider(edgeArray: Array[SimpleEdge]) extends EdgeProvider[SimpleEdge] {
    def getEdges = edgeArray.toIterator
  }

  def main(args: Array[String]) = {
    val edges = new RecursiveMAT(21, 2).getEdges.toArray
    val edgeProvider = new TestEdgeProvider(edges)
    val edgeProviders = edges.grouped(1 << 18).map(new TestEdgeProvider(_)).toSeq
    val nGroups = 1 << (18 + 3 - 18)

    println("            % 6d thread % 6d threads    elements  same".format(1, nGroups))
    print("TrieMap     ")
    val tm0 = TrieMap[Long, Int]()
    val tm1 = TrieMap[Long, Int]()
    val e00 = { () =>
      for (Edge(u, v) <- edgeProvider.getEdges) {
        tm0(u) = tm0.getOrElse(u, 0) + 1
        tm0(v) = tm0.getOrElse(v, 0) + 1
      }
    }.elapsed
    print("% 10d ms  ".format(e00))

    val e01 = { () =>
      for (ep <- edgeProviders.par; Edge(u, v) <- ep.getEdges) tm1.synchronized {
        tm1(u) = tm1.getOrElse(u, 0) + 1
        tm1(v) = tm1.getOrElse(v, 0) + 1
      }
    }.elapsed
    print("% 10d ms  ".format(e01))

    val same0 = tm0.sameElements(tm1)
    println("% 10d  %s".format(tm0.size, same0))

    print("Map         ")
    val m0 = Map[Long, Int]()
    val m1 = Map[Long, Int]()
    val e10 = { () =>
      for (Edge(u, v) <- edgeProvider.getEdges) {
        m0(u) = m0.getOrElse(u, 0) + 1
        m0(v) = m0.getOrElse(v, 0) + 1
      }
    }.elapsed
    print("% 10d ms  ".format(e10))

    val e11 = { () =>
      for (ep <- edgeProviders.par; Edge(u, v) <- ep.getEdges) m1.synchronized {
        m1(u) = m1.getOrElse(u, 0) + 1
        m1(v) = m1.getOrElse(v, 0) + 1
      }
    }.elapsed
    print("% 10d ms  ".format(e11))

    val same1 = m0.forall { case (k, v) => m1(k) == v }
    println("% 10d  %s".format(m0.size, same1))

    print("GrowingArray")
    val a0 = GrowingArray[Int](0)
    val a1 = GrowingArray[Int](0)
    val e20 = { () =>
      for (Edge(u, v) <- edgeProvider.getEdges) {
        a0(u) = a0(u) + 1
        a0(v) = a0(v) + 1
      }
    }.elapsed
    print("% 10d ms  ".format(e20))

    val e21 = { () =>
      for (ep <- edgeProviders.par; Edge(u, v) <- ep.getEdges) a1.synchronized {
        a1(u) = a1(u) + 1
        a1(v) = a1(v) + 1
      }
    }.elapsed
    print("% 10d ms  ".format(e21))

    val same2 = a0.data.flatten.zipWithIndex.forall { case (v, k) => a1(k) == v }
    println("% 10d  %s".format(a0.nUpdated, same2))
  }
}
