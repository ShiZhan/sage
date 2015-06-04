/**
 * @author Zhan.Shi
 * @year 2015
 * @name SAGE Project
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */
object sage {
  import configuration.Options
  import graph.{ Importer, Processor, Remapper, Generator }
  import helper.Resource

  lazy val usage = Resource.getString("functions.txt")

  def main(args: Array[String]) = {
    val options = Options.getOptions(args.toList)
    if (options.isEmpty) println(usage)
    else {
      val iFile = options.getString('infile, "")
      val oFile = options.getString('outfile, "")
      val mFile = options.getString('remap, "")
      val nScan = options.getInt('nscan, 1)
      val algorithm = options.getString('process, "")
      val generator = options.getString('generate, "")
      val b = options.getBool('binary)
      val l = options.getBool('selfloop)
      val d = options.getBool('bidirectional)
      val u = options.getBool('uniq)
      if (options.getBool('help)) println(usage)
      else if (options.getBool('import)) Importer.run(iFile, l, d, b)
      else if (options.getBool('process)) Processor.run(iFile, nScan, algorithm)
      else if (options.getBool('remap)) Remapper.run(iFile, mFile, oFile, b)
      else if (options.getBool('generate)) Generator.run(generator, oFile, b)
    }
  }
}