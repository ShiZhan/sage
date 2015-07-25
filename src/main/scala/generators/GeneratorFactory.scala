package generators

/**
 * @author Zhan
 * RMAT, ER, SW, BA (Simplified), Grid{2|3}
 */
object GeneratorFactory {
  import graph.{ SimpleEdge, EdgeProvider }

  class EmptyGenerator extends EdgeProvider[SimpleEdge] {
    def getEdges = Iterator.empty
  }

  def getGenerator(genOpt: String) = genOpt.split(":").toList match {
    case "rmat" :: scale :: degree :: Nil =>
      new RecursiveMAT(scale.toInt, degree.toInt)
    case "er" :: scale :: degree :: Nil =>
      new ErdosRenyiSimplified(scale.toInt, degree.toInt)
    case "sw" :: scale :: neighbhour :: rewiring :: Nil =>
      new SmallWorld(scale.toInt, neighbhour.toInt, rewiring.toDouble)
    case "ba" :: scale :: m0 :: Nil =>
      new BarabasiAlbertSimplified(scale.toInt, m0.toInt)
    case "grid" :: rScale :: cScale :: Nil =>
      new Grid2(rScale.toInt, cScale.toInt)
    case "grid" :: xScale :: yScale :: zScale :: Nil =>
      new Grid3(xScale.toInt, yScale.toInt, zScale.toInt)
    case _ =>
      println(s"Incorrect/incomplete generator option: [$genOpt]")
      new EmptyGenerator
  }
}
