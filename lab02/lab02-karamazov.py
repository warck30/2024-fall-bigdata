from pyspark.sql import SparkSession
import re
from nltk.corpus import stopwords
import nltk
from nltk.stem import SnowballStemmer

spark = SparkSession.builder.appName("LiteraryWorkAnalysis").master("local[*]").getOrCreate()
sc = spark.sparkContext

nltk.download('stopwords')
stop_words = set(stopwords.words('russian'))

text_file = "karamazov.txt"
text_rdd = sc.textFile(text_file)

def clean_text(line):
    line = line.lower()
    line = re.sub(r'[^\w\s]', '', line)
    words = line.split()
    words = [word for word in words if word not in stop_words]
    return words

cleaned_rdd = text_rdd.flatMap(clean_text)
cleaned_rdd.cache()

def word_count(rdd):
    return rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

word_count_rdd = word_count(cleaned_rdd)

sorted_word_count_rdd = word_count_rdd.sortBy(lambda x: x[1], ascending=False)

def get_top_n_words(rdd, n, ascending=False):
    return rdd.sortBy(lambda x: x[1], ascending=ascending).take(n)

top_50_most_common = get_top_n_words(sorted_word_count_rdd, 50, ascending=False)
top_50_least_common = get_top_n_words(sorted_word_count_rdd, 50, ascending=True)

stemmer = SnowballStemmer('russian')

def stem_words(word):
    return stemmer.stem(word)

stemmed_rdd = cleaned_rdd.map(stem_words)
stemmed_rdd.cache()

stemmed_word_count_rdd = word_count(stemmed_rdd)

sorted_stemmed_word_count_rdd = stemmed_word_count_rdd.sortBy(lambda x: x[1], ascending=False)

top_50_most_common_stemmed = get_top_n_words(sorted_stemmed_word_count_rdd, 50, ascending=False)
top_50_least_common_stemmed = get_top_n_words(sorted_stemmed_word_count_rdd, 50, ascending=True)

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

spark.stop()