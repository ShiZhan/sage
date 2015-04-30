/**
 * @author Zhan.Shi
 * @year 2015
 * @name SAGE Project
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */
object sage {
  import graph.{ Importer, Processer, Remapper }
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
      case "-m" :: mapfile :: more =>
        nextOption(map ++ Map('remap -> mapfile), more)
      case "--shards" :: value :: more =>
        nextOption(map ++ Map('nShard -> value.toInt), more)
      case "--job" :: job :: more =>
        nextOption(map ++ Map('job -> job), more)
      case string :: opt :: more if isSwitch(opt) =>
        nextOption(map ++ Map('infile -> string), optList.tail)
      case string :: Nil => map ++ Map('infile -> string)
    }
  }

  def runCmd(cmd: String, inFile: String, mapFile: String, nShard: Int, jobOpt: String) = cmd match {
    case "help" => println(usage)
    case "import" => Importer.run(inFile, nShard)
    case "process" => Processer.run(inFile, nShard, jobOpt)
    case "remap" => Remapper(mapFile).remapCSV(inFile)
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
        else if (options.contains('remap)) "remap"
        else ""
      val inFile = options.getOrElse('infile, "").asInstanceOf[String]
      val mapFile = options.getOrElse('remap, "").asInstanceOf[String]
      val nShard = options.getOrElse('nShard, 1).asInstanceOf[Int]
      val jobOpt = options.getOrElse('job, "print").asInstanceOf[String]
      if ((nShard & (nShard - 1)) != 0)
        println("shards must be power of 2")
      else
        runCmd(cmd, inFile, mapFile, nShard, jobOpt)
    }
  }
}
