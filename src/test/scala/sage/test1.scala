package sage

object GeneratorTest {
  import generators._
  import helper.Timing._

  def main(args: Array[String]) = {
    val edges0 = new RecursiveMAT(16, 16).getIterator
    val (nEdge0, e0) = { () => (0 /: edges0) { (r, e) => r + 1 } }.elapsed
    val speed0 = nEdge0 / e0
    println("RMAT  16 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge0, e0, speed0))
    val edges1 = new ErdosRenyiSimplified(16, 16).getIterator
    val (nEdge1, e1) = { () => (0 /: edges1) { (r, e) => r + 1 } }.elapsed
    val speed1 = nEdge1 / e1
    println("ER(S) 16 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge1, e1, speed1))
    val edges2 = new ErdosRenyi(12, 0.01).getIterator
    val (nEdge2, e2) = { () => (0 /: edges2) { (r, e) => r + 1 } }.elapsed
    val speed2 = nEdge2 / e2
    println("ER    12 0.01   generated %9d edges in %8d ms %8d K edges/second".format(nEdge2, e2, speed2))
    val edges3 = new SmallWorld(16, 16, 0.1).getIterator
    val (nEdge3, e3) = { () => (0 /: edges3) { (r, e) => r + 1 } }.elapsed
    val speed3 = nEdge3 / e3
    println("SW    16 16 0.1 generated %9d edges in %8d ms %8d K edges/second".format(nEdge3, e3, speed3))
    val edges4 = new BarabasiAlbertSimplified(16, 16).getIterator
    val (nEdge4, e4) = { () => (0 /: edges4) { (r, e) => r + 1 } }.elapsed
    val speed4 = nEdge4 / e4
    println("BA(S) 16 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge4, e4, speed4))
    val edges5 = new BarabasiAlbert(10, 16).getIterator
    val (nEdge5, e5) = { () => (0 /: edges5) { (r, e) => r + 1 } }.elapsed
    val speed5 = nEdge5 / e5
    println("BA    10 16     generated %9d edges in %8d ms %8d K edges/second".format(nEdge5, e5, speed5))
  }
}

object EdgeFileTest {
  import scala.util.Random
  import graph.{ Edge, EdgeFile }

  val edges = Array.fill(256)(Edge(Random.nextInt(128), Random.nextInt(128)))

  def main(args: Array[String]) = {
    val edgeFileName = args.head
    val edgeFile = EdgeFile(edgeFileName)
    edgeFile.put(edges.toIterator)
    println("--- total ---")
    val total = edgeFile.total
    println(total)
    println("--- head 3 ---")
    edgeFile.getRange(0, 3).foreach { println }
    println("--- next 3 ---")
    edgeFile.getRange(3, 3).foreach { println }
    println("--- tail 3 ---")
    edgeFile.getRange(total - 3, 3).foreach { println }
    println("--- all ---")
    val sum = (0 /: edgeFile.get) { (r, i) => r + 1 }
    println("should be same as total")
    println(sum + " " + (total == sum))
    edgeFile.close
  }
}

object EdgeSortTest {
  import scala.util.Random
  import graph.{ Edge, Edges, Importer }
  import Importer.sortEdges
  import Edges.EdgesWrapper

  val V = 1 << 15
  val E = 1 << 20
  val edges = Iterator.continually(Edge(Random.nextInt(V), Random.nextInt(V))).take(E)

  def main(args: Array[String]) = {
    import configuration.Options.getCache
    val sorted = sortEdges(edges, 1 << 22)
    sorted.toText("out.edges")
  }
}

object ParseOptionsTest {
  import configuration.Options
  def main(args: Array[String]) = {
    val options = Options.getOptions(args.toList)
    val iFile = options.getString('infile, "")
    val oFile = options.getString('outfile, "")
    val mFile = options.getString('remap, "")
    val vFile = options.getString('vdbfile, "")
    val algorithm = options.getString('process, "")
    val generator = options.getString('generate, "")
    val b = options.getBool('binary)
    val l = options.getBool('selfloop)
    val d = options.getBool('bidirectional)
    val s = options.getBool('sort)
    val u = options.getBool('uniq)
    println("iFile:     " + iFile)
    println("oFile:     " + oFile)
    println("mFile:     " + mFile)
    println("vFile:     " + vFile)
    println("algorithm: " + algorithm)
    println("generator: " + generator)
    println("binary:    " + b)
    println("selfloop:  " + l)
    println("bidirect:  " + b)
    println("sort:      " + s)
    println("uniq:      " + u)
    println("cache:     " + Options.getCache.name)
  }
}