package configuration

/**
 * @author ShiZhan
 * parsing program options and prepare default configuration
 */
object Options {
  type OptionMap = Map[Symbol, Any]
  type FileNames = List[String]

  class OptionMapWrapper(options: (OptionMap, FileNames)) {
    val (om, fns) = options
    def isEmpty = om.isEmpty
    def allowSelfloop = om.contains('selfloop)
    def isBidirectional = om.contains('bidirectional)
    def isBinary = om.contains('binary)
    def isWeighted = om.contains('weight)
    def isMultiThread = om.contains('multithread)
    def runHelper = om.contains('help)
    def runImporter = om.contains('import)
    def runProcessor = om.contains('process)
    def runGenerator = om.contains('generate)
    def runRemapper = om.contains('remap)

    def getAlgOpt = om.getOrElse('process, "").asInstanceOf[String]
    def getGenOpt = om.getOrElse('generate, "").asInstanceOf[String]
    def getMFName = om.getOrElse('remap, "").asInstanceOf[String]

    def getFileName = if (fns.isEmpty) "" else fns.head
    def getFileNames = fns

    def getSpecifiedInt(s: Symbol, d: Int)(checker: Int => Boolean) = om.get(s) match {
      case Some(value: Int) if checker(value) => value
      case _ => d
    }
  }

  private def nextOption(optList: List[String], om: OptionMap, fns: FileNames): (OptionMap, FileNames) =
    optList match {
      case "-h" :: more =>
        nextOption(more, om ++ Map('help -> true), fns)
      case "-i" :: more =>
        nextOption(more, om ++ Map('import -> true), fns)
      case "-p" :: algorithm :: more =>
        nextOption(more, om ++ Map('process -> algorithm), fns)
      case "-m" :: mapFile :: more =>
        nextOption(more, om ++ Map('remap -> mapFile), fns)
      case "-g" :: generator :: more =>
        nextOption(more, om ++ Map('generate -> generator), fns)
      case "--binary" :: more =>
        nextOption(more, om ++ Map('binary -> true), fns)
      case "--self-loop" :: more =>
        nextOption(more, om ++ Map('selfloop -> true), fns)
      case "--bidirectional" :: more =>
        nextOption(more, om ++ Map('bidirectional -> true), fns)
      case "--weight" :: more =>
        nextOption(more, om ++ Map('weight -> true), fns)
      case "--multi-thread" :: more =>
        nextOption(more, om ++ Map('multithread -> true), fns)
      case fileName :: more =>
        nextOption(more, om, fileName :: fns)
      case _ => (om, fns)
    }

  def getOptions(optList: List[String]) = {
    val options = nextOption(optList, Map(), List())
    new OptionMapWrapper(options)
  }
}
