package sage.test

object ConfigurationTest {
  import configuration.{ Environment, Options }
  def main(args: Array[String]) = {
    val options = Options.getOptions(args.toList)
    val iFile = options.getString('infile, "")
    val oFile = options.getString('outfile, "")
    val mFile = options.getString('remap, "")
    val vFile = options.getString('vdbfile, "")
    val nScan = options.getInt('nscan, 1)
    val algorithm = options.getString('process, "")
    val generator = options.getString('generate, "")
    val b = options.getBool('binary)
    val l = options.getBool('selfloop)
    val d = options.getBool('bidirectional)
    println("iFile:     " + iFile)
    println("oFile:     " + oFile)
    println("mFile:     " + mFile)
    println("vFile:     " + vFile)
    println("nScan:     " + nScan)
    println("algorithm: " + algorithm)
    println("generator: " + generator)
    println("binary:    " + b)
    println("selfloop:  " + l)
    println("bidirect:  " + b)
    println("cache:     " + Environment.cachePath)
  }
}