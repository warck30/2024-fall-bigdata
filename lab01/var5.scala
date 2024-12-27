object UniqueWordsApp extends App {
  def getText(): String = {
    val text = scala.io.StdIn.readLine("Введите текст: ")
    text
  }

  // Функция для разбиения текста на слова и удаления дубликатов
  def getUniqueWords(text: String): List[String] = {
    text.split("\\s+").toList.toSet.toList
  }

  val text = getText()
  val uniqueWords = getUniqueWords(text)

  println("Уникальные слова:")
  uniqueWords.foreach(println)
}
