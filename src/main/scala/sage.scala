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
    case "-i" :: Nil => Importer.console2bin
    case "-i" :: inputFileName :: Nil => Importer.file2bin(inputFileName)
    case "-p" :: inputFileName :: options => new Scanner(inputFileName).run(options)
    case _ => println(incorrectArgs)
  }
}
