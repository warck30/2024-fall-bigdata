from pyspark.sql import SparkSession
import re
from nltk.corpus import stopwords
import nltk
from nltk.stem import SnowballStemmer

# Инициализация Spark
spark = SparkSession.builder.appName("LiteraryWorkAnalysis").master("local[*]").getOrCreate()
sc = spark.sparkContext

# Загрузка стоп-слов из NLTK
nltk.download('stopwords')
stop_words = set(stopwords.words('russian'))

# Загрузка текстового файла
text_file = "karamazov.txt"
text_rdd = sc.textFile(text_file)

# Функция для очистки текста
def clean_text(line):
    line = line.lower()
    line = re.sub(r'[^\w\s]', '', line)
    words = line.split()
    words = [word for word in words if word not in stop_words]
    return words

# Очистка текста
cleaned_rdd = text_rdd.flatMap(clean_text)
cleaned_rdd.cache()

# WordCount
def word_count(rdd):
    return rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

word_count_rdd = word_count(cleaned_rdd)

# Сортировка по количеству
sorted_word_count_rdd = word_count_rdd.sortBy(lambda x: x[1], ascending=False)

# Топ 50 самых распространенных и наименее распространенных слов
def get_top_n_words(rdd, n, ascending=False):
    return rdd.sortBy(lambda x: x[1], ascending=ascending).take(n)

top_50_most_common = get_top_n_words(sorted_word_count_rdd, 50, ascending=False)
top_50_least_common = get_top_n_words(sorted_word_count_rdd, 50, ascending=True)

# Инициализация стеммера
stemmer = SnowballStemmer('russian')

# Функция для стемминга слов
def stem_words(word):
    return stemmer.stem(word)

# Стемминг очищенного RDD
stemmed_rdd = cleaned_rdd.map(stem_words)
stemmed_rdd.cache()

# WordCount на стеммированных словах
stemmed_word_count_rdd = word_count(stemmed_rdd)

# Сортировка по количеству
sorted_stemmed_word_count_rdd = stemmed_word_count_rdd.sortBy(lambda x: x[1], ascending=False)

# Топ 50 самых распространенных и наименее распространенных слов после стемминга
top_50_most_common_stemmed = get_top_n_words(sorted_stemmed_word_count_rdd, 50, ascending=False)
top_50_least_common_stemmed = get_top_n_words(sorted_stemmed_word_count_rdd, 50, ascending=True)

# Запись результатов в файл
with open('output.txt', 'w', encoding='utf-8') as f:
    f.write("Топ 50 самых распространенных слов:\n")
    for word, count in top_50_most_common:
        f.write(f"{word}: {count}\n")

    f.write("\nТоп 50 наименее распространенных слов:\n")
    for word, count in top_50_least_common:
        f.write(f"{word}: {count}\n")

    f.write("\nТоп 50 самых распространенных слов после стемминга:\n")
    for word, count in top_50_most_common_stemmed:
        f.write(f"{word}: {count}\n")

    f.write("\nТоп 50 наименее распространенных слов после стемминга:\n")
    for word, count in top_50_least_common_stemmed:
        f.write(f"{word}: {count}\n")

# Завершение Spark сессии
spark.stop()
