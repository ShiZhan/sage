/**
 * @author Zhan.Shi
 * @year 2015
 * @name SAGE Project
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */
object sage {
  import graph.{ Importer, Processor, Remapper, Generator }
  import helper.{ Resource, Utils }

  lazy val usage = Resource.getString("functions.txt")

  type OptionMap = Map[Symbol, Any]

  def isSwitch(s: String) = s.startsWith("-")

  def nextOption(map: OptionMap, optList: List[String]): OptionMap = {
    optList match {
      case "-h" :: more =>
        nextOption(map ++ Map('help -> true), more)
      case "-i" :: more =>
        nextOption(map ++ Map('import -> true), more)
      case "-p" :: algorithm :: more =>
        nextOption(map ++ Map('process -> algorithm), more)
      case "-m" :: mapfile :: more =>
        nextOption(map ++ Map('remap -> mapfile), more)
      case "-g" :: generator :: more =>
        nextOption(map ++ Map('generate -> generator), more)
      case "--shard" :: value :: more =>
        nextOption(map ++ Map('nShard -> value.toInt), more)
      case "--self-loop" :: more =>
        nextOption(map ++ Map('selfloop -> true), more)
      case "--uniq" :: more =>
        nextOption(map ++ Map('uniq -> true), more)
      case "--reverse" :: more =>
        nextOption(map ++ Map('reverse -> true), more)
      case "--out" :: outFile :: more =>
        nextOption(map ++ Map('outfile -> outFile), more)
      case inFile :: opt :: more if isSwitch(opt) =>
        nextOption(map ++ Map('infile -> inFile), optList.tail)
      case inFile :: Nil => map ++ Map('infile -> inFile)
      case _ => map
    }
  }

  def main(args: Array[String]) = {
    val options = nextOption(Map(), args.toList)
    if (options.isEmpty) println(usage)
    else {
      val inFile = options.getOrElse('infile, "").asInstanceOf[String]
      val outFile = options.getOrElse('outfile, "").asInstanceOf[String]
      val mapFile = options.getOrElse('remap, "").asInstanceOf[String]
      val nShard = Utils.getPowerOf2OrElse(options.getOrElse('nShard, 1).asInstanceOf[Int], 1)
      val algorithm = options.getOrElse('process, "").asInstanceOf[String]
      val generator = options.getOrElse('generate, "").asInstanceOf[String]
      val selfloop = options.contains('selfloop)
      val uniq = options.contains('uniq)
      val reverse = options.contains('reverse)
      if (options.contains('help)) println(usage)
      else if (options.contains('import)) Importer.run(inFile, nShard, selfloop, uniq, reverse)
      else if (options.contains('process)) Processor.run(inFile, nShard, algorithm)
      else if (options.contains('remap)) Remapper.run(inFile, mapFile, outFile)
      else if (options.contains('generate)) Generator.run(generator, outFile)
    }
  }
}
