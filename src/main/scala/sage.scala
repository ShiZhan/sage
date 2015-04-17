object sage {
  import helper.Resource

  lazy val usage = Resource.getString("functions.txt")
  val incorrectArgs = "Incorrect parameters, see help (sage -h)."

  def main(args: Array[String]) = args.toList match {
    case "-h" :: Nil => println(usage)
    case "-i" :: fileName :: Nil => println(fileName)
    case Nil => println("read edge list from console input")
    case _ => println(incorrectArgs)
  }
}
