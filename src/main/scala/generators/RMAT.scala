package generators

import java.util.concurrent.ThreadLocalRandom
import scala.collection.JavaConversions._
import graph.{ Edge, SimpleEdge, EdgeProvider }

class RecursiveMAT(scale: Int, degree: Long) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 31 && degree > 0)

  val E = (1L << scale) * degree
  val edgeIDs = Iterator.iterate(0L)(_ + 1L).takeWhile { _ <= E }

  def dice(i: Int) = i match {
    case d if (d < 57) => (0, 0)
    case d if (d < 76) => (1, 0)
    case d if (d < 95) => (0, 1)
    case _ => (1, 1)
  }

  def nextEdge = {
    val r = ThreadLocalRandom.current()
    val dices = r.ints(scale, 0, 99).iterator().map(_.toInt).map(dice)
    val (u, v) = ((0, 0) /: dices) { (p0, p1) =>
      val (x0, y0) = p0; val (x1, y1) = p1
      ((x0 << 1) + x1, (y0 << 1) + y1)
    }
    Edge(u, v)
  }

  def getEdges = for (
    g <- edgeIDs.grouped(1 << 13);
    edgeID <- g.par
  ) yield nextEdge
}
