package generators

class ErdosRenyi(scale: Int, ratio: Double) {
  require(ratio < 1 && ratio > 0)
  import scala.util.Random

  val total = 1L << scale
  val range = 256
  val probability = (range * ratio).toInt

  def vertices = {
    var vID = -1L
    Iterator.continually { vID += 1; vID }.takeWhile(_ < total)
  }

  def getIterator = for (u <- vertices; v <- vertices if Random.nextInt(range) < probability)
    yield graph.Edge(u, v)
}