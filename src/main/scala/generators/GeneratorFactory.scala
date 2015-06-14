package generators

/**
 * @author Zhan
 * RMAT, ER, SW, BA (Simplified), Grid{2|3}
 */
import graph.Edge

abstract class AbstractGenerator {
  def getEdges: Iterator[Edge]
}

object GeneratorFactory {
  class EmptyGenerator extends AbstractGenerator {
    def getEdges = Iterator[Edge]()
  }

  def optParser(genOpt: String) = genOpt.split(":").toList match {
    case "rmat" :: scale :: degree :: Nil =>
      new RecursiveMAT(scale.toInt, degree.toInt)
    case "er" :: scale :: ratio :: Nil =>
      new ErdosRenyi(scale.toInt, ratio.toDouble)
    case "ers" :: scale :: degree :: Nil =>
      new ErdosRenyiSimplified(scale.toInt, degree.toInt)
    case "sw" :: scale :: neighbhour :: rewiring :: Nil =>
      new SmallWorld(scale.toInt, neighbhour.toInt, rewiring.toDouble)
    case "ba" :: scale :: m0 :: Nil =>
      new BarabasiAlbert(scale.toInt, m0.toInt)
    case "bas" :: scale :: m0 :: Nil =>
      new BarabasiAlbertSimplified(scale.toInt, m0.toInt)
    case "grid" :: rScale :: cScale :: Nil =>
      new Grid2(rScale.toInt, cScale.toInt)
    case "grid" :: xScale :: yScale :: zScale :: Nil =>
      new Grid3(xScale.toInt, yScale.toInt, zScale.toInt)
    case _ =>
      println(s"Unknown generator option: [$genOpt]")
      new EmptyGenerator
  }
}