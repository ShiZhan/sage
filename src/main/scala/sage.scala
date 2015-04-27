/**
 * @author Zhan.Shi
 * @year 2015
 * @name SAGE Project
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */
object sage {
  import graph.{ Importer, Scanner }
  import helper.Resource

  lazy val usage = Resource.getString("functions.txt")
  val incorrectArgs = "Incorrect parameters, see help (sage -h)."

  def main(args: Array[String]) = args.toList match {
    case "-h" :: Nil => println(usage)
    case "-i" :: Nil => Importer().run
    case "-i" :: inputFileNames => inputFileNames.foreach(Importer(_).run)
    case "-r" :: inputFileName :: options => Scanner(inputFileName).run(options)
    case _ => println(incorrectArgs)
  }
}
