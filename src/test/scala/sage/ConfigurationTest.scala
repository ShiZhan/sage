package sage.test

object ConfigurationTest {
  import configuration.{ Environment, Options }
  def main(args: Array[String]) = {
    val options = Options.getOptions(args.toList)
    val eFile = options.getString('efile, "")
    val mFile = options.getString('mfile, "")
    val algorithm = options.getString('process, "")
    val generator = options.getString('generate, "")
    val b = options.getBool('binary)
    val l = options.getBool('selfloop)
    val d = options.getBool('bidirectional)
    val w = options.getBool('weight)
    println("eFile:     " + eFile)
    println("mFile:     " + mFile)
    println("algorithm: " + algorithm)
    println("generator: " + generator)
    println("binary:    " + b)
    println("selfloop:  " + l)
    println("bidirect:  " + d)
    println("weight:    " + w)
    println("cache:     " + Environment.cachePath)
  }
}