package sage.test

object EdgeFileTest {
  import scala.util.Random
  import graph.{ Edge, SimpleEdgeFile }

  val edges = Array.fill(1 << 16)(Edge(Random.nextInt(128), Random.nextInt(128)))

  def main(args: Array[String]) = {
    val edgeFile =
      if (args.isEmpty) {
        val f = new SimpleEdgeFile("edgefile-test.bin")
        f.putEdges(edges.toIterator)
        f
      } else new SimpleEdgeFile(args.head)
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
    val sum = (0 /: edgeFile.getEdges) { (r, i) => r + 1 }
    println("should be same as total")
    println(sum + " " + (total == sum))
    edgeFile.close
  }
}
