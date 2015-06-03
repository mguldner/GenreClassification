package imdb_project

/**
 * Created by mathieu on 03/06/15.
 */

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object Preparation {

  case class Movie(title : String, year: Integer, genre : String)
  case class PlotSummary(title: String, year: Integer, synopsis: String)

  private val genresPath: String = "/home/mathieu/Travail/TUBerlin/AIM3/GitHub/aim3/Project/genres.list"

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // read the text file
    val movieLines = env.readTextFile(genresPath)

    val movies = compute(movieLines)

    movies.writeAsText("/tmp/testImdb", writeMode = WriteMode.OVERWRITE)

  }

  def compute(lines: DataSet[String]) : DataSet[Movie] = {
    lines.map(parseToMovie(_))
      .filter(_!=null)
  }

  def parseToMovie(line : String) = {
    val genre_pattern = "^(?![\\s\"])(.+)\\s+\\((\\d{4})\\)\\s+([^\\(\\)][A-Za-z\\-]+)$".r
    line.trim match {
      case genre_pattern(title, year, genre) => Movie(title.toString, year.toInt, genre.toString)
      case _ => null
    }
  }


  //sort per genre
}