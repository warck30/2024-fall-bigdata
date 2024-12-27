# Лабораторные работы по курсу "Большие данные"

## **Lab01**

> Необходимо разработать приложение на **Scala** с использованием
>  **функционального подхода**. Для Текст может быть задан константой или введен пользователем. Необходимо создать список, содержащий только
> уникальные элементы исходного.

- Запуск кода:

```shell
scala  var5.scala
```

- Листинг вывода
```
Введите текст: 123 qwe qwe q ddd ddd ddd d
Уникальные слова:
ddd
123
qwe
q
d
```
---

## **Lab02**

> Разработка приложения для Apache Spark в автономном режиме
> Проанализируйте литературное произведение с помощью Spark RDD:

```
Федор Михайлович Достоевский – Братья Карамазовы
```
>  1. Произвести очистку текста (удалить стоп-слова, исправить проблемы с данными и т. д.);
>  2. Разработать WordCount по вашему тексту;
>  3. Выведите Top50 наиболее и наименее распространенных слов;
>  4. Сделайте стемминг для вашего текста;
>  5. Выведите Top50 наиболее и наименее распространенных слов по результатам стемминга.

- Запуск кода

> **! Важно** |
> Необходимо виртулаьное окружение venv с установленными пакетами pip

```shell
pip  install  pyspark  nltk
python3  lab02-karamazov.py
```

В результате появится файл с тектовым выводом

---

## **Lab03**

> Проанализируйте набор данных с помощью Spark SQL и инструментов визуализации. Вы должны самостоятельно представить не менее 5 различных упражнений с данными (как минимум 3 из них должны иметь какую-либо визуализацию). 

Датасет
```
Общественное питание в Москве
```
[Портал открытых данных правительства Москвы](https://data.mos.ru/opendata/1903)

- Лабораторная работа проведена на мощностях google colab
- Исходные .ipynb и .py файлы в репозитории /lab03/*