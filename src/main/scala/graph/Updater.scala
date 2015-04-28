package graph

object Updater extends helper.Logging {
  import scala.collection.mutable.BitSet
  private val shards2update = new BitSet()
  shards2update.add(0)
  shards2update.add(0)
  shards2update.add(1)
  shards2update.add(1)
  shards2update.add(2)
  shards2update.add(2)
  val scanners = shards2update.map { s => Scanner(s"$s") }
  // scanners.par.foreach { _.run(List[String]()) } // if multiple devices
  scanners.foreach { _.run(List[String]()) }
}
