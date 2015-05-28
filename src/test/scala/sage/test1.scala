package sage

object ReadEdgeList {
  import graph.Edges
  def main(args: Array[String]) = {
    val edgeFileName = args.head
    val edgeFile = Edges.fromFile(edgeFileName)
    println("--- total ---")
    val total = edgeFile.total
    println(total)
    println("--- head 3 ---")
    edgeFile.range(0, 3).foreach { println }
    println("--- next 3 ---")
    edgeFile.range(3, 3).foreach { println }
    println("--- tail 3 ---")
    edgeFile.range(total - 3, 3).foreach { println }
    println("--- all ---")
    val sum = (0 /: edgeFile.all) { (r, i) => r + 1 }
    println("should be same as total")
    println(sum + " " + (total == sum))
    edgeFile.close
  }
}

object ParseOptions {
  import configuration.Options
  def main(args: Array[String]) = {
    val options = Options.getOptions(args.toList)
    val tempD = options.getString('tempDir, Options.tempDirDefault)
    println(tempD)
  }
}