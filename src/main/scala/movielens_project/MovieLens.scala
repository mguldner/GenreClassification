package movielens_project

/**
 * Created by mathieu on 03/06/15.
 */

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

case class Movie(id: Int, title : String, genres : String)
case class Rating(userId: Int, movieId: Int, rating: Double)
case class Link(movieId: Int, imdbId: Int, tmdbId: Int)
case class Tag(userId: Int, movieId: Int, tag: String)

object MovieLensItems {

  private val moviesPath: String = "/home/mathieu/Travail/TUBerlin/IMPRO3/MovieLens_Usecase/Datasets/ml-latest-small/movies.csv"
  private val ratingsPath: String = "/home/mathieu/Travail/TUBerlin/IMPRO3/MovieLens_Usecase/Datasets/ml-latest-small/ratings.csv"
  //private val linksPath: String = "/home/mathieu/Travail/TUBerlin/IMPRO3/MovieLens_Usecase/Datasets/ml-latest-small/links.csv"
  //private val tagsPath: String = "/home/mathieu/Travail/TUBerlin/IMPRO3/MovieLens_Usecase/Datasets/ml-latest-small/tags.csv"

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val movies: DataSet[Movie] = getMovies(env);
    val ratings: DataSet[Rating] = getRatings(env)
    //val links: DataSet[Link] = getLinks(env)
    //val tags: DataSet[Tag] = getTags(env)

    // The average rating for each movie --> (movieId, Rating)
    val avgRatingByMovie: DataSet[(Int, Double)] = {
      ratings
        .groupBy("movieId")
        .reduceGroup{
        (in, out : Collector[(Int, Double)]) =>
          var sum=0.0
          var count=0
          var key=0
          in.foreach(r => {
            sum+=r.rating
            count+=1
            key=r.movieId
          })
          out.collect(key, sum/count)
      }
    }

    // Add the title of the movie --> (movieTitle, Rating)
    val moviesAndAverageRatings = avgRatingByMovie.join(movies).where(0).equalTo(0).map{mr => (mr._2.title, mr._1._2)}

    moviesAndAverageRatings.writeAsCsv("/tmp/avg", writeMode = WriteMode.OVERWRITE)

    /*val best20MoviesByUserWithRatings: DataSet[(Int, Int, Double)] = getBestNMoviesByUserWithRatings(env, 20)

    val best20MoviesByUserWithTitle : DataSet[(Int, String)] = {
      best20MoviesByUserWithRatings.join(movies).where(1).equalTo(0).map(x => (x._1._1, x._2.title))
    }

    best20MoviesByUserWithRatings.writeAsCsv("/tmp/userRatings", writeMode = WriteMode.OVERWRITE)
    best20MoviesByUserWithTitle.writeAsCsv("/tmp/top", writeMode = WriteMode.OVERWRITE)
    val recom = getBestRecommandationsForUserAccordingOthers(env, 1)
    recom.writeAsCsv("/tmp/recom", writeMode = WriteMode.OVERWRITE)*/
    env.execute("movieLens")
  }

  private def getBestNMoviesByUserWithRatings(env: ExecutionEnvironment, n: Int): DataSet[(Int, Int, Double)] = {
    getRatings(env)
      .groupBy("userId")
      .sortGroup("rating", org.apache.flink.api.common.operators.Order.DESCENDING)
      .first(n).map{r => (r.userId, r.movieId, r.rating)}
  }

  private  def getRatedMoviesByUser(env: ExecutionEnvironment, rating: Double): DataSet[(Int, Int, Double)] = {
    getRatings(env).map(x => (x.userId, x.movieId, x.rating)).filter(_._3 == rating)
  }

  /*private def getBestRecommandationsForUserAccordingOthers(env: ExecutionEnvironment, userId: Int): DataSet[(Int, Double)] = {
    val best3MoviesForUser: DataSet[(Int, Double)] = {
      getBestNMoviesByUserWithRatings(env, 3)
        .filter(_._1 == userId)
        .map(x => (x._2, x._3))
    }

    /* Attach a score to each user to know the best links */
    val scoreForOthers: DataSet[(Int, Double)] = {
      getBestNMoviesByUserWithRatings(env, 20).filter(_._3 > 4.0).groupBy(_._1) // Best high rated movies for each user : DataSet[(IdUser, IdMovie, Rating)]
        .reduceGroup{
        (in, out:Collector[(Int, Double)]) =>
          var score = 0.0
          var key = 0
          in.foreach(r=>{
            if(r._1 != userId){
              key = r._1
              for( (currentMovie, currentRating) <- best3MoviesForUser){
                var coef = 0.0
                if(r._2==currentMovie)
                  coef = currentRating
                else
                  coef = 2.5
                score += (r._3 * coef)
              }
            }
          })
          out.collect(key, score)
      }
    }

    //val best3Links: DataSet[(Int, Double)] = scoreForOthers.
  }*/

  private def getMovies(env: ExecutionEnvironment): DataSet[Movie] = {
    env.readCsvFile[(Int, String, String)](
      moviesPath, ignoreFirstLine = true,
      includedFields = Array(0,1,2)).map{m => new Movie(m._1, m._2, m._3)}
  }

  private def getRatings(env : ExecutionEnvironment): DataSet[Rating] = {
    env.readCsvFile[(Int, Int, Double)](
      ratingsPath, ignoreFirstLine = true,
      includedFields = Array(0,1,2)).map{r => new Rating(r._1, r._2, r._3)}
  }

  /*private def getLinks(env:ExecutionEnvironment): DataSet[Link] = {
    env.readCsvFile[(Int, Int, Int)](
      linksPath, "\n", ',', true, null, false,
      includedFields = Array(0,1,2)).map{l => new Link(l._1, l._2, l._3)}
  }

  private def getTags(env: ExecutionEnvironment): DataSet[Tag] = {
    env.readCsvFile[(Int, Int, String)](
      tagsPath, "\n", ',', true, null, false,
      includedFields = Array(0,1,2)).map{t => new Tag(t._1, t._2, t._3)}
  }*/
}