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
    println("iFile: " + iFile)
    println("oFile: " + oFile)
    println("mFile: " + mFile)
    println("vFile: " + vFile)
    println("algorithm: " + algorithm)
    println("generator: " + generator)
    println("binary: " + b)
    println("selfloop: " + l)
    println("bidirectional: " + b)
    println("sort: " + s)
    println("uniq: " + u)
    println(Options.cachePath)
  }
}