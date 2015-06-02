package configuration

/**
 * @author ShiZhan
 * environment variables and related settings.
 */
object Environment {
  import java.io.File
  import scala.util.Properties.{ envOrElse, envOrNone, userDir }

  val pwd = userDir

  def cachePath = {
    val tempOrPwd = envOrElse("TEMP", pwd)
    val tempOrPwdFile = new File(tempOrPwd)
    val sageCache = envOrNone("SAGE_CACHE")
    val cacheDir = sageCache match {
      case Some(s) =>
        val sageCacheFile = new File(s)
        if (sageCacheFile.exists()) {
          if (sageCacheFile.isDirectory()) sageCacheFile else tempOrPwdFile
        } else { sageCacheFile.mkdirs(); sageCacheFile }
      case _ => tempOrPwdFile
    }
    cacheDir.getAbsolutePath
  }

  def cacheFile = {
    val cacheFileName = "%016x.tmp".format(compat.Platform.currentTime)
    new File(cachePath, cacheFileName)
  }
}