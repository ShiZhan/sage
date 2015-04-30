/**
 * @author Zhan.Shi
 * @year 2015
 * @name SAGE Project
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */
object sage {
  import graph.{ Importer, Processer, Remapper }
  import graph.Shards.nShardShouldBePowerOf2
  import helper.Resource

  lazy val usage = Resource.getString("functions.txt")

  type OptionMap = Map[Symbol, Any]

  def isSwitch(s: String) = s.startsWith("-")

  def nextOption(map: OptionMap, optList: List[String]): OptionMap = {
    optList match {
      case "-h" :: more =>
        nextOption(map ++ Map('help -> true), more)
      case "-i" :: more =>
        nextOption(map ++ Map('import -> true), more)
      case "-p" :: more =>
        nextOption(map ++ Map('process -> true), more)
      case "-m" :: mapfile :: more =>
        nextOption(map ++ Map('remap -> mapfile), more)
      case "--shard" :: value :: more =>
        nextOption(map ++ Map('nShard -> value.toInt), more)
      case "--job" :: job :: more =>
        nextOption(map ++ Map('job -> job), more)
      case string :: opt :: more if isSwitch(opt) =>
        nextOption(map ++ Map('infile -> string), optList.tail)
      case string :: Nil => map ++ Map('infile -> string)
      case _ => map
    }
  }

  def main(args: Array[String]) = {
    val options = nextOption(Map(), args.toList)
    if (options.isEmpty) println(usage)
    else {
      val inFile = options.getOrElse('infile, "").asInstanceOf[String]
      val mapFile = options.getOrElse('remap, "").asInstanceOf[String]
      val nShard = options.getOrElse('nShard, 1).asInstanceOf[Int].toPowerOf2
      val jobOpt = options.getOrElse('job, "print").asInstanceOf[String]
      if (options.contains('help)) println(usage)
      else if (options.contains('import)) Importer.run(inFile, nShard)
      else if (options.contains('process)) Processer.run(inFile, nShard, jobOpt)
      else if (options.contains('remap)) Remapper.run(inFile, mapFile)
    }
  }
}
