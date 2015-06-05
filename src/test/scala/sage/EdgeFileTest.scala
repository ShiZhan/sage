package sage.test

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
