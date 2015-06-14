/**
 * Output Synthetic Graph as edge list
 */
package graph

/**
 * @author Zhan
 * Synthetic Graph Generator
 */
object Generator extends helper.Logging {
  import generators.GeneratorFactory
  import Edges.EdgesWrapper

  def run(genOpt: String, outFile: String, binary: Boolean) = {
    val generator = GeneratorFactory.optParser(genOpt)
    val edges = generator.getEdges
    if (outFile.isEmpty) edges.foreach(println)
    else {
      logger.info("START")
      if (binary) edges.toFile(outFile) else edges.toText(outFile)
      logger.info("COMPLETE")
    }
  }
}