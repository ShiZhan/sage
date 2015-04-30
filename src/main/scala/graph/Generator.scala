/**
 * Output Synthetic Graph as edge list
 */
package graph

/**
 * @author Zhan
 * Synthetic Graph Generator
 */
object Generator {
  def run(options: String) = options.split(":").toList match {
    case _ => new RMATGenerator(8, 8).getIterator.foreach(println)
  }
}

class RMATGenerator(scale: Int, degree: Long) {
  import scala.util.Random

  val totalEdges = (1L << scale) * degree
  var nEdges = 0L
  def nextEdge = {
    nEdges += 1
    val (u, v) = ((0L, 0L) /: (1 to scale)) { (p, s) =>
      val (x0, y0) = p
      val (x, y) = (x0 << 1, y0 << 1)
      val dice = Random.nextInt(100)
      if (dice < 57) (x, y)
      else if (dice < 76) (x + 1, y)
      else if (dice < 95) (x, y + 1)
      else (x + 1, y + 1)
    }
    if (nEdges <= totalEdges) Edge(u, v) else EdgeUtils.invalidEdge
  }
  def getIterator = Iterator.continually(nextEdge).takeWhile(_.valid)
}