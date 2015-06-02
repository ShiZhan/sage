/**
 * Output Synthetic Graph as edge list
 */
package graph

/**
 * @author Zhan
 * Synthetic Graph Generator
 * RMAT, ER, SW, BA (Simplified), Grid{2|3}
 */
object Generator extends helper.Logging {
  import generators._
  import Edges.EdgesWrapper

  def run(generator: String, outFile: String, binary: Boolean) = {
    val edges = generator.split(":").toList match {
      case "rmat" :: scale :: degree :: Nil =>
        new RecursiveMAT(scale.toInt, degree.toInt).getIterator
      case "er" :: scale :: ratio :: Nil =>
        new ErdosRenyi(scale.toInt, ratio.toDouble).getIterator
      case "ers" :: scale :: degree :: Nil =>
        new ErdosRenyiSimplified(scale.toInt, degree.toInt).getIterator
      case "sw" :: scale :: neighbhour :: rewiring :: Nil =>
        new SmallWorld(scale.toInt, neighbhour.toInt, rewiring.toDouble).getIterator
      case "ba" :: scale :: m0 :: Nil =>
        new BarabasiAlbert(scale.toInt, m0.toInt).getIterator
      case "bas" :: scale :: m0 :: Nil =>
        new BarabasiAlbertSimplified(scale.toInt, m0.toInt).getIterator
      case "grid" :: rScale :: cScale :: Nil =>
        new Grid2(rScale.toInt, cScale.toInt).getIterator
      case "grid" :: xScale :: yScale :: zScale :: Nil =>
        new Grid3(xScale.toInt, yScale.toInt, zScale.toInt).getIterator
      case _ =>
        println(s"Unknown generator: [$generator]"); Iterator[Edge]()
    }
    if (outFile.isEmpty) edges.foreach(println)
    else {
      logger.info("START")
      if (binary) edges.toFile(outFile) else edges.toText(outFile)
      logger.info("COMPLETE")
    }
  }
}