package algorithms

class KCore(implicit context: Context)
    extends Algorithm[Long](context, Long.MaxValue) {
  import scala.collection.mutable.Map
  import graph.Edge
  import helper.IteratorOps.VisualOperations

  def iterations = {
    logger.info("Preparing vertex degree ...")
    //    val pool = Map[Long, Long]()
    //    def degreeIncreaseByOne(k: Long) = { val d: Long = pool.getOrElse(k, 0); pool.put(k, d + 1) }
    //    getEdges.foreachDo { case Edge(u, v) => Seq(u, v).foreach(degreeIncreaseByOne) }
    //
    //    var core = 1L
    //    val temp = Map[Long, Long]()
    //    while (!pool.isEmpty) {
    //      logger.info("Collecting core {}", core)
    //      if (pool.find { case (k, v) => v <= core } == None) core += 1
    //      else {
    //        pool.filter { case (k, v) => v <= core }
    //          .foreach { case (k, v) => pool.remove(k); temp.put(k, core) }
    //        getEdges
    //          .flatMap { e => Iterator(e, e.reverse) }
    //          .filter { case Edge(u, v) => pool.contains(u) && temp.contains(v) }
    //          .foreach { case Edge(u, v) => val value = pool.get(u).get; pool.put(u, value - 1) }
    //        data ++= temp; temp.clear()
    //      }
    //    }
  }
}