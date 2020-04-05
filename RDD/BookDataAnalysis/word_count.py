from pyspark import SparkConf
from pyspark import SparkContext


def main():
    """
    The driver function. It is responsible for applying all transformation and will print the results onto the console.

    :return:
    """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('WordCount')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/Book/book.txt')

    # We can count the words in following two ways.

    # Type - 1: We split each line into words, then we transform each word into a tuple
    # having word as key and 1 as it's value, and finally we add the count to get the total
    # number of occurrences of words
    word_count_rdd = data_rdd.flatMap(lambda line: line.split())\
                             .map(lambda _word: (_word, 1))\
                             .reduceByKey(lambda x, y: x + y)

    # Getting the results
    results = word_count_rdd.collect()

    # Displaying the results onto console
    for word, count in results:
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word, ' ', count)

    # Type - 2: We split each line into words and use a built in operator to count the
    # occurrences of words.
    word_count_map = data_rdd.flatMap((lambda line: line.split())) \
                             .countByValue()

    # Displaying the results onto console
    for word, count in word_count_map.items():
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word, ' ', count)


if __name__ == '__main__':
    main()
