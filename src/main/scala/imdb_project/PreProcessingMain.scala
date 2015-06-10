package imdb_project

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * The PreProcessingMain object has the purpose to
 * run the preprocessing pipeline of the genre classification task
 * Check out the object PreProcessing on what it does
 */
object PreProcessingMain {

  def main(args: Array[String]) {

    // input argument flags
    val genresPathFlag = "genres"
    val synopsisPathFlag = "synopses"

    // set up global parameters
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val genrePath = paramTool.getRequired(genresPathFlag)
    val synopsisPath = paramTool.getRequired(synopsisPathFlag)

    // set up exectution
    val env = ExecutionEnvironment.getExecutionEnvironment

    // create two datasets of MovieSynopsis (as (trainingSet, testSet))
    val movieSet = PreProcessing.extractMovieInfo(env.readTextFile(genrePath, "iso-8859-1"))
    movieSet
      .setParallelism(1)
      .writeAsText("file:///tmp/genreclass/genres.list", WriteMode.OVERWRITE)

    val synopsisSet = PreProcessing.extractSynopsisInfo(
        env.readFile(new CustomInputFormat("iso-8859-1", PreProcessing.synopsis_line_delim), synopsisPath)
      )
    synopsisSet
      .setParallelism(1)
      .writeAsText("file:///tmp/genreclass/plot.list", WriteMode.OVERWRITE)

    PreProcessing
      .joinSets(movieSet, synopsisSet)
      .setParallelism(1)
      .writeAsText("file:///tmp/genreclass/join.list", WriteMode.OVERWRITE)

    // run execution
    env.execute()
  }
}