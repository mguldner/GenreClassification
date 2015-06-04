package imdb_project

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
 * The PreProcessing processes the genres.list and plot.list of the IMDb
 * in order to create a dataset that combines information about a movie (genre, title, year, synopsis)
 * this dataset can be split up into 2 parts to create a training and test set
 */
object PreProcessing {

  // line delimiter for plot-summaries list
  val synopsis_line_delim = "-------------------------------------------------------------------------------"

  // patterns to extract necessary information from files
  val genre_pattern =
    "^(?![\\s\"])(.+)\\s+\\((\\d{4})\\)\\s+([^\\(\\)][A-Za-z\\-]+)$".r // 1:title, 2:year, 3:genre
  val synopsis_movie_pattern =
    "^MV:\\s+([^\"]+)\\s+\\((\\d{4})\\)\\s+(?!\\(TV\\)|\\(mini\\)|\\(VG\\)|\\(V\\))".r // 1.title, 2:year
  val synopsis_text_pattern =
    "PL:\\s*(.+)".r // 1.one line of the synopsis text

  // fraction of movies that will be in the training set
  val TRAINING_FRACTION = 0.7f


  // create the training and test set
  def preProcess(genrePath: String, synopsisPath: String, env: ExecutionEnvironment):
    (DataSet[MovieSynopsis], DataSet[MovieSynopsis]) = {

    // read files and transform to appropriate datasets
    val movieSet = extractMovieInfo(env.readTextFile(genrePath))
    val synopsisSet = extractSynopsisInfo(
      env.readFile(new CustomInputFormat("iso-8859-1", synopsis_line_delim), synopsisPath)
    )

    // join datasets in order to keep only movies that have a synopsis

    // create training set by keeping TRAINING_FRACTION of movies for each genre

    // create test set by keeping 1-TRAINING_FRACTION of movies for each genre

    // return (trainingSet, testSet)
  }

  def extractMovieInfo(lines: DataSet[String]): DataSet[Movie] = {
    lines
      .flatMap(line => genre_pattern.unapplySeq(line) match {
        case None => Seq.empty[Movie]
        case Some(m) => Seq(new Movie(m(0).toLowerCase.trim, m(1).toInt, m(2).toLowerCase.trim))
      })
  }

  def extractSynopsisInfo(lines: DataSet[String]): DataSet[Synopsis] = {
    lines
      .flatMap(line => lineToSynopsis(line))
  }

  def lineToSynopsis(line: String): Seq[Synopsis] = {

    // extract the movie title and year of the synopsis
    val titleYear = synopsis_movie_pattern.findFirstMatchIn(line) match {
      case None => return Seq.empty[Synopsis] // no or invalid movie title -> empty synopsis
      case Some(m) => (m.group(0), m.group(1)) // return tuple(title, year)
    }

    // extract text of the synopsis
    var synopsisText = ""
    synopsis_text_pattern
      .findAllIn(line)
      .foreach(mtch => synopsisText += " " + mtch)

    Seq(new Synopsis(titleYear._1.toLowerCase.trim, titleYear._2.toInt, synopsisText))
  }
}

