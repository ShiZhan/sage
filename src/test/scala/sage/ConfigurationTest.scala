package sage.test

object ConfigurationTest {
  import configuration.Options
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
    val ofn = options.getOutput.getOrElse("<N/A>")
    println("remap file: " + mFile)
    println("algorithm:  " + algorithm)
    println("generator:  " + generator)
    println("binary:     " + b)
    println("selfloop:   " + l)
    println("bidirect:   " + d)
    println("weight:     " + w)
    println("Input File Name:  " + fn)
    println("Input File Names:\n" + fns.mkString("\n"))
    println("Outout File Name: " + ofn)
  }
}