object sage {
  import helper.Resource

  lazy val usage = Resource.getString("functions.txt")
  val incorrectArgs = "Incorrect parameters, see help (sage -h)."

  def main(args: Array[String]) = args.toList match {
    case "-h" :: Nil => println(usage)
    case "-i" :: Nil => graph.Importer.console2bin
    case "-i" :: inputFileName :: Nil => graph.Importer.file2bin(inputFileName)
    case _ => println(incorrectArgs)
  }
}
