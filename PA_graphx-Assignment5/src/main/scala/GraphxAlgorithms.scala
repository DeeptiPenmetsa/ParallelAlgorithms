import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Files, Paths}


/**
  * Created by Lema on 11/3/16.
  */

object GraphxAlgorithms {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("PA").setMaster("local[*]")

    val sc = new SparkContext(sconf)
    /**
      * Reading input file of triplets format
      **/

    val input = sc.textFile("src/main/resources/sampleDataSet.csv")

    val splitInput = input.map(line => line.split(","))

    val triples = splitInput.map(line => (line(0), line(1), line(2)))

    val subjects = splitInput.map(line => line(0))

    val objects = splitInput.map(line => line(2))

    val predicates = splitInput.map(line => line(1))

    val concepts = subjects.union(objects).distinct()

    val conceptId = concepts.zipWithIndex()

    val alignConceptswithId = conceptId.map(line => (line._2, line._1))
    /**
      * vertex RDD
      **/
    val vertexRdd = alignConceptswithId

    val mapTriples = triples.map(line => (line._1, (line._2, line._3)))

    val triplesJoin = mapTriples.join(conceptId)

    val maptriplesjoin = triplesJoin.map(line => ((line._2._2), (line._2._1)))

    val orderforObjectjoin = maptriplesjoin.map(line => (line._2._2, ((line._2._1), line._1)))

    val objectsjoin = orderforObjectjoin.join(conceptId)

    val mapObjectsJoin = objectsjoin.map(line => (line._2._2, line._2._1))

    /**
      * edge RDD
      **/

    val edgeRdd = mapObjectsJoin.map(line => Edge(line._1, line._2._2, line._2._1))

    println("vertices")
    vertexRdd.foreach(println)

    println("edges:")
    edgeRdd.foreach(println)

    val graph = Graph(vertexRdd, edgeRdd)


    /**
      * page rank*/

    val ranks = graph.pageRank(0.1).vertices

    val vertexRanks = ranks.join(vertexRdd).map(line => (line._2._2, line._2._1))
    println("Page Rank:")
    vertexRanks.foreach(println)

    /**
      * connected components
      **/
    val connectedComponents = graph.connectedComponents().vertices


    println("CC:")

    connectedComponents.foreachPartition(println)

    /**
      * Traingle counting
      **/

    val trCounting = graph.triangleCount().vertices

    println("Triangle counting:")

    trCounting.foreach(println)


  }

}
