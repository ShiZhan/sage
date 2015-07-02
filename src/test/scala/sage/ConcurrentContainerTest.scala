package sage.test

object ConcurrentContainerTest {
  import java.io.File
  import scala.collection.JavaConversions._
  import scala.collection.concurrent.TrieMap
  import org.mapdb.{ BTreeMap, DBMaker }
  import graph.{ Edge, SimpleEdge, EdgeProvider }
  import generators.RecursiveMAT
  import helper.Timing._

  class TestEdgeProvider(edgeArray: Array[SimpleEdge]) extends EdgeProvider[SimpleEdge] {
    def getEdges = edgeArray.toIterator
  }

  def main(args: Array[String]) = {
    val edges = new RecursiveMAT(18, 8).getEdges.toArray
    val edgeProvider = new TestEdgeProvider(edges)
    val edgeProviders = edges.grouped(1 << 18).map(new TestEdgeProvider(_)).toSeq
    val nGroups = 1 << (18 + 3 - 18)

    println("=== TrieMap ===")
    val tm0 = TrieMap[Long, Long]()
    val tm1 = TrieMap[Long, Long]()
    println("1 thread test:")
    val e0 = { () =>
      for (Edge(u, v) <- edgeProvider.getEdges) {
        tm0(u) = tm0.getOrElse(u, 0L) + 1
        tm0(v) = tm0.getOrElse(v, 0L) + 1
      }
    }.elapsed
    println(s"1 thread: $e0 ms")

    println(s"$nGroups threads test:")
    val e1 = { () =>
      for (ep <- edgeProviders.par; Edge(u, v) <- ep.getEdges) tm1.synchronized {
        tm1(u) = tm1.getOrElse(u, 0L) + 1
        tm1(v) = tm1.getOrElse(v, 0L) + 1
      }
    }.elapsed
    println(s"$nGroups threads: $e1 ms")

    val same0 = tm0.forall { case (k, v) => tm1(k) == v }
    println(s"same results: $same0")

    println("=== MapDB ===")
    val db = DBMaker
      .fileDB(new File("mapdb-test.db")).deleteFilesAfterClose()
      .closeOnJvmShutdown()
      .make()
    val m0: BTreeMap[Long, Long] = db.treeMap("0")
    val m1: BTreeMap[Long, Long] = db.treeMap("1")

    println("1 thread test:")
    val e2 = { () =>
      for (Edge(u, v) <- edgeProvider.getEdges) {
        val d0 = m0.getOrDefault(u, 0); m0.put(u, d0 + 1)
        val d1 = m0.getOrDefault(v, 0); m0.put(v, d1 + 1)
      }
    }.elapsed
    println(s"1 thread: $e2 ms")
    db.commit()

    println(s"$nGroups threads test:")
    val e3 = { () =>
      for (ep <- edgeProviders.par; Edge(u, v) <- ep.getEdges) m1.synchronized {
        val d0 = m1.getOrDefault(u, 0); m1.put(u, d0 + 1)
        val d1 = m1.getOrDefault(v, 0); m1.put(v, d1 + 1)
      }
    }.elapsed
    println(s"$nGroups threads: $e3 ms")
    db.commit()

    val same1 = m0.forall { case (k, v) => m1.get(k) == v }
    println(s"same results: $same1")

    db.close()
  }
}
