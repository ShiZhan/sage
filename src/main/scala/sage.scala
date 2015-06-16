/**
 * @author Zhan.Shi
 * @year 2015
 * @name SAGE Project
 * @license http://www.apache.org/licenses/LICENSE-2.0
 */
object sage {
  import configuration.Options
  import graph.{ Importer, Processor, Mapper, Generator }
  import helper.Resource

  lazy val usage = Resource.getString("functions.txt")

  def main(args: Array[String]) = {
    val options = Options.getOptions(args.toList)
    if (options.isEmpty) println(usage)
    else {
      val eFile = options.getString('efile, "")
      val mFile = options.getString('mfile, "")
      val algorithm = options.getString('process, "")
      val generator = options.getString('generate, "")
      val l = options.getBool('selfloop)
      val d = options.getBool('bidirectional)
      val b = options.getBool('binary)
      if (options.getBool('help)) println(usage)
      else if (options.getBool('import)) Importer.run(eFile, l, d, b)
      else if (options.getBool('process)) Processor.run(eFile, algorithm)
      else if (options.getBool('mfile)) Mapper(mFile).map(eFile, b)
      else if (options.getBool('generate)) Generator.run(generator, eFile, b)
    }
  }
}