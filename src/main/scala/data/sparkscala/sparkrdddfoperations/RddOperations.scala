package data.sparkscala.sparkrdddfoperations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Class to demonstrate various operations on RDDs.
 * This class covers:
 * - RDD creation
 * - Transformations (e.g., map, filter, flatMap, reduceByKey, etc.)
 * - Actions (e.g., collect, count, first, take, etc.)
 *
 * @param sc The SparkContext instance used to create and operate on RDDs.
 */
class RddOperations(sc: SparkContext) {

  /**
   * Create RDDs from different sources.
   */
  def createRDDs(): Unit = {
    // 1. Create RDD from a collection
    val data = List(1, 2, 3, 4, 5)
    val rddFromCollection: RDD[Int] = sc.parallelize(data)
    println("RDD created from collection:")
    rddFromCollection.collect().foreach(println)

    // 2. Create RDD from a text file
    val rddFromFile: RDD[String] = sc.textFile("path/to/textfile.txt")
    println("RDD created from text file:")
    rddFromFile.take(10).foreach(println)
  }

  /**
   * Demonstrate RDD transformations and actions.
   */
  def demonstrateRDDOperations(): Unit = {
    // Sample data
    val data = List(
      ("Alice", 1),
      ("Bob", 2),
      ("Alice", 3),
      ("Bob", 4),
      ("Charlie", 5)
    )
    val rdd: RDD[(String, Int)] = sc.parallelize(data)

    // 1. Map: Apply a function to each element
    val mappedRDD = rdd.map { case (name, score) => (name, score * 2) }
    println("1. Map operation:")
    mappedRDD.collect().foreach(println)

    // 2. Filter: Filter elements based on a condition
    val filteredRDD = rdd.filter { case (name, score) => score > 2 }
    println("2. Filter operation:")
    filteredRDD.collect().foreach(println)

    // 3. FlatMap: Map each element to a sequence of elements
    val flatMappedRDD = rdd.flatMap { case (name, score) => Seq(name, score.toString) }
    println("3. FlatMap operation:")
    flatMappedRDD.collect().foreach(println)

    // 4. ReduceByKey: Combine values with the same key using a reduce function
    val reducedRDD = rdd.reduceByKey(_ + _)
    println("4. ReduceByKey operation:")
    reducedRDD.collect().foreach(println)

    // 5. GroupByKey: Group values by key
    val groupedRDD = rdd.groupByKey()
    println("5. GroupByKey operation:")
    groupedRDD.collect().foreach { case (key, values) => println(s"$key: ${values.mkString(", ")}") }

    // 6. Join: Join two RDDs based on keys
    val otherData = List(
      ("Alice", "A"),
      ("Bob", "B"),
      ("Charlie", "C")
    )
    val rdd2: RDD[(String, String)] = sc.parallelize(otherData)
    val joinedRDD = rdd.join(rdd2)
    println("6. Join operation:")
    joinedRDD.collect().foreach(println)

    // 7. Cogroup: Group pairs of RDDs by key
    val cogroupedRDD = rdd.cogroup(rdd2)
    println("7. Cogroup operation:")
    cogroupedRDD.collect().foreach { case (key, (values1, values2)) =>
      println(s"$key: values1 = ${values1.mkString(", ")}, values2 = ${values2.mkString(", ")}")
    }

    // 8. SortBy: Sort elements based on a key
    val sortedRDD = rdd.sortBy { case (name, score) => score }
    println("8. SortBy operation:")
    sortedRDD.collect().foreach(println)

    // 9. Collect: Return all elements of the RDD to the driver
    println("9. Collect operation:")
    rdd.collect().foreach(println)

    // 10. Count: Count the number of elements in the RDD
    val count = rdd.count()
    println(s"10. Count operation: $count")

    // 11. First: Return the first element of the RDD
    val first = rdd.first()
    println(s"11. First operation: $first")

    // 12. Take: Return the first n elements of the RDD
    val taken = rdd.take(3)
    println("12. Take operation:")
    taken.foreach(println)

    // 13. TakeOrdered: Return the first n elements, ordered by a given ordering
    val takeOrderedRDD = rdd.takeOrdered(3)(Ordering[Int].reverse.on(_._2))
    println("13. TakeOrdered operation:")
    takeOrderedRDD.foreach(println)

    // 14. SaveAsTextFile: Save the RDD to a text file
    println("14. SaveAsTextFile operation (commented out for safety):")
    // rdd.saveAsTextFile("path/to/output.txt")

    // 15. Sample: Sample rows from the RDD
    val sampledRDD = rdd.sample(withReplacement = false, fraction = 0.5)
    println("15. Sample operation:")
    sampledRDD.collect().foreach(println)

    // 16. Distinct: Remove duplicate elements
    val distinctRDD = rdd.distinct()
    println("16. Distinct operation:")
    distinctRDD.collect().foreach(println)

    // 17. Union: Combine elements from two RDDs
    val unionRDD = rdd.union(sc.parallelize(List(("Eve", 6))))
    println("17. Union operation:")
    unionRDD.collect().foreach(println)

    // 18. Intersection: Return elements common to both RDDs
    val rdd3 = sc.parallelize(List(("Alice", 1), ("Bob", 2)))
    val intersectionRDD = rdd.intersection(rdd3)
    println("18. Intersection operation:")
    intersectionRDD.collect().foreach(println)

    // 19. Subtract: Return elements of one RDD that are not in another
    val subtractRDD = rdd.subtract(rdd3)
    println("19. Subtract operation:")
    subtractRDD.collect().foreach(println)

    // 20. Coalesce: Reduce the number of partitions
    val coalescedRDD = rdd.coalesce(1)
    println("20. Coalesce operation:")
    println(s"Number of partitions after coalesce: ${coalescedRDD.partitions.length}")

    // 21. Repartition: Increase or decrease the number of partitions
    val repartitionedRDD = rdd.repartition(4)
    println("21. Repartition operation:")
    println(s"Number of partitions after repartition: ${repartitionedRDD.partitions.length}")

    // 22. Cache: Cache the RDD for faster access
    println("22. Cache operation:")
    rdd.cache().count() // Trigger caching

    // 23. Unpersist: Remove the RDD from cache
    println("23. Unpersist operation:")
    rdd.unpersist()

    // 24. ZipWithIndex: Add an index to each element
    val rddWithIndex = rdd.zipWithIndex()
    println("24. ZipWithIndex operation:")
    rddWithIndex.collect().foreach { case ((name, score), index) => println(s"($name, $score) -> Index: $index") }
  }
}
