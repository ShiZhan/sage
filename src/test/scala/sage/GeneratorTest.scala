package sage.test

object GeneratorTest {
  import generators._
  import helper.Timing._

  def main(args: Array[String]) = {
    val edges0 = new RecursiveMAT(16, 16).getEdges
    val (nEdge0, e0) = { () => (0 /: edges0) { (r, e) => r + 1 } }.elapsed
    val speed0 = nEdge0 / e0
    println("RMAT   16 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge0, e0, speed0))

    val edges1 = new ErdosRenyiSimplified(16, 16).getEdges
    val (nEdge1, e1) = { () => (0 /: edges1) { (r, e) => r + 1 } }.elapsed
    val speed1 = nEdge1 / e1
    println("ER(S)  16 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge1, e1, speed1))
    val edges2 = new ErdosRenyi(13, 16).getEdges
    val (nEdge2, e2) = { () => (0 /: edges2) { (r, e) => r + 1 } }.elapsed
    val speed2 = nEdge2 / e2
    println("ER     13 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge2, e2, speed2))

    val edges3 = new SmallWorld(16, 16, 0.1).getEdges
    val (nEdge3, e3) = { () => (0 /: edges3) { (r, e) => r + 1 } }.elapsed
    val speed3 = nEdge3 / e3
    println("SW     16 16 0.1 generated %9d edges in %8d ms %8d K edges/second".format(nEdge3, e3, speed3))

    val edges4 = new BarabasiAlbertSimplified(16, 16).getEdges
    val (nEdge4, e4) = { () => (0 /: edges4) { (r, e) => r + 1 } }.elapsed
    val speed4 = nEdge4 / e4
    println("BA(S)  16 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge4, e4, speed4))
    val edges5 = new BarabasiAlbertOverSimplified(16, 16).getEdges
    val (nEdge5, e5) = { () => (0 /: edges5) { (r, e) => r + 1 } }.elapsed
    val speed5 = nEdge5 / e5
    println("BA(OS) 16 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge5, e5, speed5))
    val edges6 = new BarabasiAlbert(10, 16).getEdges
    val (nEdge6, e6) = { () => (0 /: edges6) { (r, e) => r + 1 } }.elapsed
    val speed6 = nEdge6 / e6
    println("BA     10 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge6, e6, speed6))
  }
}
