package generators

class ErdosRenyi(scale: Int, ratio: Double) {
  require(ratio < 1 && ratio > 0)
  import scala.util.Random
  import graph.Edge

  val total = 1L << scale
  val range = 1 << 20
  val probability = (range * ratio).toInt

  def vertices = {
    var vID = -1L
    Iterator.continually { vID += 1; vID }.takeWhile(_ < total)
  }

  def getIterator = for (u <- vertices; v <- vertices if Random.nextInt(range) < probability)
    yield Edge(u, v)
}

class ErdosRenyiSimplified(scale: Int, degree: Int) {
  import scala.util.Random
  import graph.Edge

  val V = 1L << scale
  val E = V * degree
  val M = V - 1

  var nEdge = E

  def getIterator =
    Iterator.continually { nEdge -= 1; Edge(Random.nextLong & M, Random.nextLong & M) }
      .takeWhile { _ => nEdge >= 0 }
}