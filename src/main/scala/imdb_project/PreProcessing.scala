package imdb_project

import org.apache.flink.api.scala._

/**
 * The PreProcessing processes the genres.list and plot.list of the IMDb
 * in order to create a dataset that combines information about a movie (genre, title, year, synopsis)
 * this dataset can be split up into 2 parts to create a training and test set
 */
object PreProcessing {

  // line delimiter for plot-summaries list
  val synopsis_line_delim = "-------------------------------------------------------------------------------\n"

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
    val movieSet = extractMovieInfo(env.readTextFile(genrePath, "iso-8859-1"))
    val synopsisSet = extractSynopsisInfo(
      env.readFile(new CustomInputFormat("iso-8859-1", synopsis_line_delim), synopsisPath)
    )

    // join datasets in order to keep only movies that have a synopsis
    val movieSynopsis = joinSets(movieSet, synopsisSet)

    // create training set by keeping TRAINING_FRACTION of movies for each genre

    // create test set by keeping 1-TRAINING_FRACTION of movies for each genre

    // return (trainingSet, testSet)
    return (env.fromCollection(Seq(new MovieSynopsis("asd", 1, "asd", "ad"))), env.fromCollection(Seq(new MovieSynopsis("asd", 1, "asd", "ad"))))
  }

  def joinSets(movieSet: DataSet[Movie], synopsisSet: DataSet[Synopsis]): DataSet[MovieSynopsis] = {

    movieSet
      .join(synopsisSet)
      .where(m => (m.title, m.year)).equalTo(s => (s.title, s.year))
      .apply(
        (mov, syn) => MovieSynopsis(mov.title, mov.year, mov.genre, syn.synopsis)
      )//.withForwardedFields("f0.title->f0; f0.year->f1; f0.genre->f2; f1.synopsis->f3")
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
      case Some(m) => (m.group(1), m.group(2)) // return tuple(title, year)
    }

    // extract text of the synopsis
    var synopsisText = ""
    synopsis_text_pattern
      .findAllMatchIn(line)
      .foreach(mtch => synopsisText += " " + mtch.group(1))

    Seq(new Synopsis(titleYear._1.toLowerCase.trim, titleYear._2.toInt, synopsisText.toLowerCase.trim))
  }
}

