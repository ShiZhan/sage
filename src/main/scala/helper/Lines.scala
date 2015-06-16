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
  import IteratorOps.VisualOperations

  def fromConsole =
    Source.fromInputStream(System.in).getLines()

  def fromFile(fileName: String) =
    Source.fromFile(new File(fileName)).getLines()

  def fromFile(file: File) =
    Source.fromFile(file).getLines()

  implicit class LinesWrapper[Printable](lines: Iterator[Printable]) {
    def toFile(fileName: String) = {
      val pw = new PrintWriter(new File(fileName))
      lines.foreachDo(pw.println)
      pw.close()
    }
  }
}