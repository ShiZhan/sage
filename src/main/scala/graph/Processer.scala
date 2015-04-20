package graph

object Processer {
  import java.io.File
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  val db = DBMaker.newFileDB(new File(".sage-data")).closeOnJvmShutdown().make()
  val vertices = db.getTreeMap("vertices").asInstanceOf[ConcurrentNavigableMap[Long, Long]]

  def shutdown() = db.close()
}