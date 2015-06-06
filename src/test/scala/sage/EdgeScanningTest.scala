package sage.test.Edge

object EdgeScanningTest {
  import scala.util.Random
  import graph.{ Edge, Edges, Importer }
  import Edges._

  val V = 1 << 15
  val E = 1 << 20
  val edges = Iterator.continually(Edge(Random.nextInt(V), Random.nextInt(V))).take(E)

  def main(args: Array[String]) = {
    import configuration.Environment.cachePath
  }
}
