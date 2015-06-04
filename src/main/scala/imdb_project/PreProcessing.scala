package imdb_project

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
 * The PreProcessing processes the genres.list and synopsis.list of the IMDb
 * in order to create a dataset that combines information about a movie (genre, title, year, synopsis)
 * this dataset can be split up into 2 parts to create a training and test set
 */
object PreProcessing {

  // patterns to extract necessary information from files
  val genre_pattern = "^(?![\\s\"])(.+)\\s+\\((\\d{4})\\)\\s+([^\\(\\)][A-Za-z\\-]+)$".r // 1:title, 2:year, 3:genre
  val synopsis_pattern = "".r // 1.title, 2:year, 3:synopsis

  // movie and synopsis datasets
  var movieSet: DataSet[Movie] = _
  var synopsisSet: DataSet[Synopsis] = _

  def readFiles(genrePath: String, synopsisPath: String, env: ExecutionEnvironment): Unit = {

    // read and set movie set

    // read and set synopsis set
  }
}

