package sage

object ReadEdgeList {
  import graph.Edges
  def main(args: Array[String]) = {
    val edgeFile = args.head
    println("--- total ---")
    val total = Edges.fromBinTotal(edgeFile)
    println(total)
    println("--- head 3 ---")
    Edges.fromBinRange(edgeFile, 0, 3).foreach { println }
    println("--- next 3 ---")
    Edges.fromBinRange(edgeFile, 3, 3).foreach { println }
    println("--- tail 3 ---")
    Edges.fromBinRange(edgeFile, total - 3, 3).foreach { println }
  }
}
