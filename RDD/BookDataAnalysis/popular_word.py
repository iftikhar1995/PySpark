import re

from pyspark import SparkConf
from pyspark import SparkContext


def normalize_words(text: str) -> list:
    """
    The function will split the text into words. Apart from this, it removes
    all the punctuations and convert the word into lowercase.

    :param text: The line from the data file
    :type text: str
    :return: The list of normalize words
    :rtype: list
    """

    return re.compile(r'\W+', re.UNICODE).split(text.lower())


def main():
    """
    The driver function. It is responsible for applying all transformation and will print the results onto the console.

    :return:
    """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('PopularWordCount')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/Book/book.txt')

    # We split each line into words, then we transform each word into a tuple
    # having word as key and 1 as it's value. After that we add the count to get the total
    # number of occurrences of words, then we transform the word, count tuple into count,
    # word tuple and finally we sort the RDD by keys.
    sorted_word_count_rdd = data_rdd.flatMap(normalize_words) \
                                    .map(lambda _word: (_word, 1)) \
                                    .reduceByKey(lambda x, y: x + y)\
                                    .map(lambda x: (x[1], x[0]))\
                                    .sortByKey(ascending=False)

    # Collecting the results
    results = sorted_word_count_rdd.collect()

    # Displaying the results onto console
    for count, word in results:
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word, ' ', count)


if __name__ == '__main__':
    main()
