package generators

class RecursiveMAT(scale: Int, degree: Long) extends AbstractGenerator {
  require(scale > 0 && scale < 32 && degree > 0)
  import java.util.concurrent.ThreadLocalRandom
  import graph.Edge

  val totalEdges = (1L << scale) * degree
  val groupEdges = 1 << 13
  val groups = totalEdges >> 13

  def dice(d: Int) =
    if (d < 57) (0, 0) else if (d < 76) (1, 0) else if (d < 95) (0, 1) else (1, 1)

  def nextEdge = {
    val tlRandom = ThreadLocalRandom.current()
    val dices = Seq.fill(scale)(tlRandom.nextInt(100)).map(dice)
    val (u, v) = ((0L, 0L) /: dices) { (p0, p1) =>
      val (x0, y0) = p0; val (x1, y1) = p1
      ((x0 << 1) + x1, (y0 << 1) + y1)
    }
    Edge(u, v)
  }

  def getEdges = {
    var n = 0L
    if (groups > 0)
      Iterator.continually(n).takeWhile { _ => n += 1; n <= groups }
        .flatMap { n => (1 to groupEdges).par.map { _ => nextEdge }.toIterator }
    else
      Iterator.continually(nextEdge).takeWhile { _ => n += 1; n <= totalEdges }
  }
}