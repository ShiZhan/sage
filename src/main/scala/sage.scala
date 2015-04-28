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

  type OptionMap = Map[Symbol, Any]

  def isSwitch(s: String) = s.startsWith("-")

  def nextOption(map: OptionMap, optList: List[String]): OptionMap = {
    optList match {
      case Nil => map
      case "-h" :: more =>
        nextOption(map ++ Map('help -> true), more)
      case "-i" :: more =>
        nextOption(map ++ Map('import -> true), more)
      case "-p" :: more =>
        nextOption(map ++ Map('process -> true), more)
      case "--shards" :: value :: more =>
        nextOption(map ++ Map('shards -> value.toInt), more)
      case "--job" :: job :: more =>
        nextOption(map ++ Map('job -> job), more)
      case string :: opt :: more if isSwitch(opt) =>
        nextOption(map ++ Map('infile -> string), optList.tail)
      case string :: Nil => map ++ Map('infile -> string)
    }
  }

  def runCmd(cmd: String, inFile: String, shards: Int, jobOpt: String) = cmd match {
    case "help" => println(usage)
    case "import" =>
      println("import from " + { if (inFile == "") "console" else inFile } + " to " + shards + " shards")
    case "process" =>
      if (inFile == "") println("need imported graph")
      else for (s <- 0 to (shards - 1)) {
        println("processing [%s%03d.bin] with [%s]".format(inFile, s, jobOpt))
      }
    case _ => println("Unknown command")
  }

  def main(args: Array[String]) = {
    val options = nextOption(Map(), args.toList)
    if (options.isEmpty) println(usage)
    else {
      val cmd =
        if (options.contains('help)) "help"
        else if (options.contains('import)) "import"
        else if (options.contains('process)) "process"
        else ""
      val inFile = options.getOrElse('infile, "").asInstanceOf[String]
      val shards = options.getOrElse('shards, 1).asInstanceOf[Int]
      val jobOpt = options.getOrElse('job, "print").asInstanceOf[String]
      runCmd(cmd, inFile, shards, jobOpt)
    }
  }
}
