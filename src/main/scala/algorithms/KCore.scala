package algorithms

class KCore(implicit context: Context)
    extends Algorithm[Long](context) {
  import scala.collection.JavaConversions._
  import graph.Edge
  import helper.Gauge.IteratorOperations

  def iterations = {
    logger.info("Preparing vertex degree ...")
    val pool = step(0)
    def degreeIncreaseByOne(k: Long) = { val d: Long = pool.getOrElse(k, 0); pool.put(k, d + 1) }
    E.get.foreachDo { case Edge(u, v) => Seq(u, v).foreach(degreeIncreaseByOne) }

    var core = 1L
    val temp = step(1)
    while (!pool.isEmpty) {
      logger.info("Collecting core {}", core)
      if (pool.find { case (k, v) => v <= core } == None) core += 1
      else {
        pool.filter { case (k, v) => v <= core }
          .foreach { case (k, v) => pool.remove(k); temp.put(k, core) }
        E.get
          .flatMap { e => Iterator(e, e.reverse) }
          .filter { case Edge(u, v) => pool.containsKey(u) && temp.containsKey(v) }
          .foreach { case Edge(u, v) => val value = pool.get(u); pool.put(u, value - 1) }
        data.putAll(temp); temp.clear()
      }
    }
  }
}