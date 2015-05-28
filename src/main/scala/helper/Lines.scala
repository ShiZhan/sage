/**
 * input/output line iterator from/to file|console
 */
package helper

/**
 * @author Zhan
 * get/put lines from text file or console
 * omit file name for console input/output
 */
object Lines {
  import scala.io.Source
  import java.io.{ File, PrintWriter }
  import helper.Gauge.IteratorOperations

  def fromConsole =
    Source.fromInputStream(System.in).getLines()

  def fromFile(fileName: String) =
    Source.fromFile(new File(fileName)).getLines()

  def fromFileOrConsole(fileName: String) =
    if (fileName.isEmpty()) fromConsole else fromFile(fileName)

  def toFile[T](lines: Iterator[T], fileName: String) = fileName match {
    case "" =>
      lines.foreach(println)
    case _ =>
      val of = new File(fileName)
      val pw = new PrintWriter(of)
      lines.foreachDo(pw.println)
      pw.close()
  }
}