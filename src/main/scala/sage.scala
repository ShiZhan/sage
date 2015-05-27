/**
 * @author Zhan.Shi
 * @year 2015
 * @name SAGE Project
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */
object sage {
  import graph.{ Importer, Processor, Remapper, Generator }
  import helper.{ Resource, Miscs }

  lazy val usage = Resource.getString("functions.txt")

  type OptionMap = Map[Symbol, Any]

  implicit class OptionMapWrapper(om: OptionMap) {
    def getString(s: Symbol, d: String) = om.getOrElse(s, d).asInstanceOf[String]
    def getInt(s: Symbol, d: Int) = om.getOrElse(s, d).asInstanceOf[Int]
    def getSpecifiedInt(s: Symbol, checker: Int => Boolean, d: Int) = om.get(s) match {
      case Some(value) if value.isInstanceOf[Int] =>
        val i = value.asInstanceOf[Int]
        if (checker(i)) i else d
      case _ => d
    }
  }

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
      case "--bidirectional" :: more =>
        nextOption(map ++ Map('bidirectional -> true), more)
      case "--vdb" :: vdbFile :: more =>
        nextOption(map ++ Map('vdbfile -> vdbFile), more)
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
      val inFile = options.getString('infile, "")
      val outFile = options.getString('outfile, "")
      val mapFile = options.getString('remap, "")
      val vdbFile = options.getString('vdbfile, "")
      val nShard = options.getSpecifiedInt('nShard, Miscs.isPowerOf2, 1)
      val algorithm = options.getString('process, "")
      val generator = options.getString('generate, "")
      val selfloop = options.contains('selfloop)
      val bidirectional = options.contains('bidirectional)
      if (options.contains('help)) println(usage)
      else if (options.contains('import)) Importer.run(inFile, selfloop, bidirectional)
      else if (options.contains('process)) Processor.run(inFile, nShard, vdbFile, algorithm)
      else if (options.contains('remap)) Remapper.run(inFile, mapFile, outFile)
      else if (options.contains('generate)) Generator.run(generator, outFile)
    }
  }
}