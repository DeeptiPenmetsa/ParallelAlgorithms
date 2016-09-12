import org.apache.spark.SparkContext

/**
  * Created by DEEPU on 9/10/2016.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object InvertedIndexTest {
  def main(args: Array[String]) = {
    val sc = new SparkContext(
      "local", "Inverted Index")
    sc.textFile("data")
      .map { line =>
        val array = line.split("\t", 2)
        (array(0), array(1))
      }
      .flatMap {
        case (path, text) =>
          // If we don't trim leading whitespace, the regex split creates
          // an undesired leading "" word!
          text.trim.split("""\W+""") map (word => (word, path))
      }
      .map {
        case (word, path) => ((word, path), 1)
      }
      .reduceByKey{
        (count1, count2) => count1 + count2
      }
      .map {
        case ((word, path), n) => (word, (path, n))
      }
      .groupBy {
        case (word, (path, n)) => word
      }
      // New: sort by Key (word).
      .sortByKey(ascending = true)
      .map {
        case (word, seq) =>
          val seq2 = seq.map {
            case (redundantWord, (path, n)) => (path, n)
          }.toSeq
            // New: sort the sequence by count, descending,
            // and also by path so that tests pass predictably!
            .sortBy {
            case (path, n) => (-n, path)
          }
          (word, seq2.mkString(", "))
      }
      .saveAsTextFile("output")
    sc.stop()
  }
}