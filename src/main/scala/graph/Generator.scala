/**
 * Output Synthetic Graph as edge list
 */
package graph

/**
 * @author Zhan
 * Synthetic Graph Generator
 */
object Generator extends helper.Logging {
  import Edges.EdgesWrapper

  def run(genOpt: String, outFile: String, binary: Boolean) = {
    val edges = generators.GeneratorFactory.optParser(genOpt).getEdges
    logger.debug("START")
    if (binary && !outFile.isEmpty) edges.toFile(outFile) else edges.toText(outFile)
    logger.debug("COMPLETE")
  }
}