package generators

class SmallWorld(scale: Int, neighbour: Int, rewiring: Double) {
  require(scale > 0
    && neighbour > 0 && neighbour < (1L << (scale - 1))
    && rewiring < 1 && rewiring > 0)
  import scala.util.Random

  val total = 1L << scale
  val range = 256
  val probability = (range * rewiring).toInt

  def vertices = {
    var vID = -1L
    Iterator.continually { vID += 1; vID }.takeWhile(_ < total)
  }

  def neighbours(id: Long) = (1 to neighbour).toIterator.map { n =>
    if (Random.nextInt(range) < probability)
      (id + Random.nextLong) & (total - 1)
    else
      (id + n) & (total - 1)
  }

  def getIterator = for (u <- vertices; v <- neighbours(u))
    yield graph.Edge(u, v)
}