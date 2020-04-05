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
    conf = SparkConf().setMaster('local').setAppName('NormalizedWordCount')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/Book/book.txt')

    # We split each line into words and use a built in operator to count the
    # occurrences of words.
    word_count_map = data_rdd.flatMap(normalize_words) \
                             .countByValue()

    # Displaying the results onto console
    for word, count in word_count_map.items():
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word, ' ', count)


if __name__ == '__main__':
    main()
