package generators

import graph.{ Edge, SimpleEdge, EdgeProvider }

class RecursiveMAT(scale: Int, degree: Long) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 31 && degree > 0)
  import java.util.concurrent.ThreadLocalRandom

  val totalEdges = (1L << scale) * degree
  val edgeIDs = {
    var eID = 0L
    Iterator.continually(eID).takeWhile { _ => eID += 1; eID <= totalEdges }
  }

  def dice(d: Int) =
    if (d < 57) (0, 0) else if (d < 76) (1, 0) else if (d < 95) (0, 1) else (1, 1)

  def nextEdge = {
    val tlRandom = ThreadLocalRandom.current()
    val dices = Seq.fill(scale)(tlRandom.nextInt(100)).map(dice)
    val (u, v) = ((0, 0) /: dices) { (p0, p1) =>
      val (x0, y0) = p0; val (x1, y1) = p1
      ((x0 << 1) + x1, (y0 << 1) + y1)
    }
    Edge(u, v)
  }

  def getEdges =
    edgeIDs.grouped(1 << 13).map(_.par.map { _ => nextEdge }.toIterator ).flatten
}
