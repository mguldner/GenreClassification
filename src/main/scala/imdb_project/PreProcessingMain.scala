package imdb_project

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * The PreProcessingMain object has the purpose to
 * run the preprocessing pipeline of the genre classification task
 * Check out the object PreProcessing on what it does
 */
object PreProcessingMain {

  def main(args: Array[String]) {

    // set up exectution
    val env = ExecutionEnvironment.getExecutionEnvironment

    // create two datasets of MovieSynopsis (as training and test set)


    // run execution
    env.execute()
  }
}