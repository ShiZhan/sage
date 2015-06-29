package sage.test

object ConfigurationTest {
  import configuration.{ Environment, Options }
  def main(args: Array[String]) = {
    val options = Options.getOptions(args.toList)
    val mFile = options.getMFName
    val algorithm = options.getAlgOpt
    val generator = options.getGenOpt
    val b = options.isBinary
    val l = options.allowSelfloop
    val d = options.isBidirectional
    val w = options.isWeighted
    val fn = options.getFileName
    val fns = options.getFileNames
    println("remap file: " + mFile)
    println("algorithm:  " + algorithm)
    println("generator:  " + generator)
    println("binary:     " + b)
    println("selfloop:   " + l)
    println("bidirect:   " + d)
    println("weight:     " + w)
    println("cache:      " + Environment.cachePath)
    println("Input/Output File Name: " + fn)
    println("Input/Output File Names:\n" + fns.mkString("\n"))
  }
}