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

  // training and test set
  var trainingSet: DataSet[MovieSynopsis] = _
  var testSet: DataSet[MovieSynopsis] = _

  // fraction of movies that will be in the training set
  val TRAINING_FRACTION = 0.7f

  
  // create the training and test set
  def preProcess(genrePath: String, synopsisPath: String, env: ExecutionEnvironment): Unit = {

  }
}

