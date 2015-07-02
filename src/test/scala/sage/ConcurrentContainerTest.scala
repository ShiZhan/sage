package sage.test

object ConcurrentContainerTest {
  import java.io.File
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker
  import graph.{ SimpleEdge, EdgeProvider }
  import generators.RecursiveMAT
  import helper.Timing._

  class TestEdgeProvider(edgeArray: Array[SimpleEdge]) extends EdgeProvider[SimpleEdge] {
    def getEdges = edgeArray.toIterator
  }

  def main(args: Array[String]) = {
    val edges = new RecursiveMAT(18, 8).getEdges.toArray
    val edgeProvider = new TestEdgeProvider(edges)
    val edgeProviders = edges.grouped(1 << 18).map(new TestEdgeProvider(_)).toSeq
    val db = DBMaker
      .fileDB(new File("mapdb-test.db")).deleteFilesAfterClose()
      .closeOnJvmShutdown().transactionDisable() //.cacheSize(1000)
      .make()
    val treeMap = db.treeMap("0")

    db.commit()
    db.close()
  }
}
