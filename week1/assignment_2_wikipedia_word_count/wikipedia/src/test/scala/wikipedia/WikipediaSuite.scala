package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*

class WikipediaSuite extends munit.FunSuite:
  def initializeWikipediaRanking(): Boolean =
    try
      WikipediaRanking
      true
    catch
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false

  import WikipediaRanking.*

  /**
    * Creates a truncated string representation of a list, adding ", ...)" if there
    * are too many elements to show
    * @param l The list to preview
    * @param n The number of elements to cut it at
    * @return A preview of the list, containing at most n elements.
    */
  def previewList[A](l: List[A], n: Int = 10): String =
    if l.length <= n then l.toString
    else l.take(n).toString.dropRight(1) + ", ...)"

  /**
    * Asserts that all the elements in a given list and an expected list are the same,
    * regardless of order. For a prettier output, given and expected should be sorted
    * with the same ordering.
    * @param actual The actual list
    * @param expected The expected list
    * @tparam A Type of the list elements
    */
  def assertSameElements[A](actual: List[A], expected: List[A]): Unit =
    val givenSet = actual.toSet
    val expectedSet = expected.toSet

    val unexpected = givenSet -- expectedSet
    val missing = expectedSet -- givenSet

    val noUnexpectedElements = unexpected.isEmpty
    val noMissingElements = missing.isEmpty

    val noMatchString =
      s"""
         |Expected: ${previewList(expected)}
         |Actual:   ${previewList(actual)}""".stripMargin

    assert(noUnexpectedElements,
      s"""|$noMatchString
          |The given collection contains some unexpected elements: ${previewList(unexpected.toList, 5)}""".stripMargin)

    assert(noMissingElements,
      s"""|$noMatchString
          |The given collection is missing some expected elements: ${previewList(missing.toList, 5)}""".stripMargin)

  // Conditions:
  // (1) the language stats contain the same elements
  // (2) they are ordered (and the order doesn't matter if there are several languages with the same count)
  def assertEquivalentAndOrdered(actual: List[(String, Int)], expected: List[(String, Int)]): Unit =
    // (1)
    assertSameElements(actual, expected)
    // (2)
    assert(
      !(actual zip actual.tail).exists({ case ((_, occ1), (_, occ2)) => occ1 < occ2 }),
      "The given elements are not in descending order"
    )

  test("'occurrencesOfLang' should work for (specific) RDD with one element: 1") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val res = (occurrencesOfLang("Java", rdd) == 1)
    assert(res, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
  }

  test("'rankLangs' should work for RDD with two elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great"), WikipediaArticle("2", "Java is OK, but Scala is cooler")))
    val ranked = rankLangs(langs, rdd)
    val res = ranked.head._1 == "Scala"
    assert(res)
  }

  test("'rankLangs' should work for RDD with many elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    val langs = List("Scala", "Java", "Python", "Go")
    val rdd = sc.parallelize(List(
      WikipediaArticle("1", "Scala is great as with Python"),
      WikipediaArticle("2", "Java is OK, but Scala is cooler"),
      WikipediaArticle("4", "Go Go Go is in the same article, Scala and Python are better but not Java"),
      WikipediaArticle("5", "Python"),
      WikipediaArticle("6", "Scala"),
      WikipediaArticle("7", "No langs")
    ))
    val ranked = rankLangs(langs, rdd)
    assertEquals(ranked, List(("Scala", 4), ("Python", 3),  ("Java", 2), ("Go", 1)))
  }

  test("'makeIndex' creates a simple index with two entries") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    val langs = List("Scala", "Java")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    assertSameElements(index.map((x, y) => (x, y.map(a => a.title).toList.mkString(","))).collect().toList, List(("Java", "2"), ("Scala", "2,3")))
  }

  test("'rankLangsUsingIndex' should work for a simple RDD with three elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    val langs = List("Scala", "Java")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    assertSameElements(index.map((x, y) => (x, y.map(a => a.title).toList.mkString(","))).collect().toList, List(("Java", "2"), ("Scala", "2,3")))
  }

  test("'rankLangsReduceByKey' should work for a simple RDD with five elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","The cool kids like Haskell more than Java"),
        WikipediaArticle("5","Java is for enterprise developers")
      )
    val rdd = sc.parallelize(articles)
    val ranked = rankLangsReduceByKey(langs, rdd)
    val res = (ranked.head._1 == "Java")
    assertEquals(ranked, List(("Java", 3), ("Scala", 2), ("Erlang", 1), ("Groovy", 1), ("Haskell", 1)))
  }

  import scala.concurrent.duration.given
  override val munitTimeout = 100.seconds
