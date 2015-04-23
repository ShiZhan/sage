package graph

object Updater extends helper.Logging {
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
  private val vertex = db.getTreeMap("vertex").asInstanceOf[ConcurrentNavigableMap[Long, Long]]
  private val update = db.getTreeMap("update").asInstanceOf[ConcurrentNavigableMap[Long, Long]]

  def commit() = db.commit()
  def shutdown() = db.close()
}