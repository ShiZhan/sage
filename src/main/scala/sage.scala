object sage {
  import graph.{ Importer, Processer }
  import helper.Resource

  lazy val usage = Resource.getString("functions.txt")
  val incorrectArgs = "Incorrect parameters, see help (sage -h)."

  def main(args: Array[String]) = args.toList match {
    case "-h" :: Nil => println(usage)
    case "-i" :: Nil =>
      Importer.console2bin
    case "-i" :: inputFileName :: Nil =>
      Importer.file2bin(inputFileName)
    case "-p" :: inputFileName :: options =>
      val p = new Processer(inputFileName)
      p.run(options)
      p.shutdown()
    case _ => println(incorrectArgs)
  }
}
