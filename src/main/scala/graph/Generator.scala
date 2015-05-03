/**
 * Output Synthetic Graph as edge list
 */
package graph

/**
 * @author Zhan
 * Synthetic Graph Generator
 * RMAT, ER
 */
object Generator {
  def run(options: String) = {
    val edges = options.split(":").toList match {
      case "rmat" :: scale :: degree :: Nil =>
        new RMATGenerator(scale.toInt, degree.toInt).getIterator
      case "er" :: scale :: ratio :: Nil =>
        new ERGenerator(scale.toInt, ratio.toDouble).getIterator
      case _ =>
        new RMATGenerator(8, 8).getIterator
    }
    edges.foreach(println)
  }
}

class RMATGenerator(scale: Int, degree: Long) {
  require(scale > 0 && scale < 32 && degree > 0)
  import scala.util.Random

  val totalEdges = (1L << scale) * degree
  var nEdges = 0L

  def dice(d: Int) =
    if (d < 57) (0, 0) else if (d < 76) (1, 0) else if (d < 95) (0, 1) else (1, 1)

  def nextEdge = {
    nEdges += 1
    val dices = Array.fill(scale)(Random.nextInt(100)).map(dice)
    val (u, v) = ((0L, 0L) /: dices) { (p0, p1) =>
      val (x0, y0) = p0; val (x1, y1) = p1; ((x0 << 1) + x1, (y0 << 1) + y1)
    }
    if (nEdges <= totalEdges) Edge(u, v) else EdgeUtils.invalidEdge
  }

  def getIterator = Iterator.continually(nextEdge).takeWhile(_.valid)
}

class ERGenerator(scale: Int, ratio: Double) {
  require(ratio < 1 && ratio > 0)
  import scala.util.Random

  val totalVertices = 1L << scale

  def getVertexItr(total: Long) = {
    var vID = -1L
    Iterator.continually { vID += 1; vID }.takeWhile(_ < total)
  }

  def getIterator = getVertexItr(totalVertices).flatMap { u =>
    for (v <- getVertexItr(totalVertices) if (Random.nextDouble() < ratio)) yield Edge(u, v)
  }
}