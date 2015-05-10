/**
 * Output Synthetic Graph as edge list
 */
package graph

/**
 * @author Zhan
 * Synthetic Graph Generator
 * RMAT, ER, SW, BA, Grid{2|3}
 */
object Generator {
  import EdgeUtils.EdgesWriter
  import generators._

  def run(generator: String, outFile: String) = {
    val edges = generator.split(":").toList match {
      case "rmat" :: scale :: degree :: Nil =>
        new RecursiveMAT(scale.toInt, degree.toInt).getIterator
      case "er" :: scale :: ratio :: Nil =>
        new ErdosRenyi(scale.toInt, ratio.toDouble).getIterator
      case "sw" :: scale :: neighbhour :: rewiring :: Nil =>
        new SmallWorld(scale.toInt, neighbhour.toInt, rewiring.toDouble).getIterator
      case "ba" :: scale :: m0 :: Nil =>
        new BarabasiAlbert(scale.toInt, m0.toInt).getIterator
      case "grid" :: rScale :: cScale :: Nil =>
        new Grid2(rScale.toInt, cScale.toInt).getIterator
      case "grid" :: xScale :: yScale :: zScale :: Nil =>
        new Grid3(xScale.toInt, yScale.toInt, zScale.toInt).getIterator
      case _ =>
        println(s"Unknown generator: [$generator]"); Iterator[Edge]()
    }
    edges.toFile(outFile)
  }
}