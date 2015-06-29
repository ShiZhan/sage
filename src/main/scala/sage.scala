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
      val eFile = options.getFileName
      val eFiles = options.getFileNames
      val mFile = options.getMFName
      val algorithm = options.getAlgOpt
      val generator = options.getGenOpt
      val l = options.allowSelfloop
      val d = options.isBidirectional
      val b = options.isBinary
      val w = options.isWeighted
      if (options.runImporter) Importer.run(eFile, l, d, b, w)
      else if (options.runProcessor) Processor.run(eFile, algorithm)
      else if (options.runGenerator) Generator.run(generator, eFile, b, w)
      else if (options.runRemapper) Mapper(mFile).map(eFile, b)
      else println(usage)
    }
  }
}
