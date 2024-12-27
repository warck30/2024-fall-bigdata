object UniqueWordsApp extends App {

  def getText(): String = {
    scala.io.StdIn.readLine("Введите текст: ")
  }

  def splitTextIntoWords(text: String): List[String] = {
    text.split("\\s+").toList
  }

  def removeDuplicates(words: List[String]): List[String] = {
    words.toSet.toList
  }

  def printWords(words: List[String]): Unit = {
    println("Уникальные слова:")
    words.foreach(println)
  }

  def main(): Unit = {
    val text = getText()
    val words = splitTextIntoWords(text)
    val uniqueWords = removeDuplicates(words)
    printWords(uniqueWords)
  }

  main()
}
