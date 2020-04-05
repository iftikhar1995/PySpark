from pyspark import SparkConf
from pyspark import SparkContext
from collections import OrderedDict


def main():
    """
    The driver function. It is responsible for applying all transformation and will print the results onto the console.

    :return:
    """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Setting the log level to ERROR
    sc.setLogLevel(logLevel='ERROR')

    # Extracting data from the data file into RDD
    data = sc.textFile("../../Resources/MovieLens/u.data")

    # Extracting the ratings from the data
    ratings = data.map(lambda line: line.split()[2])

    # Getting the Count of each unique value in the data RDD
    result = ratings.countByValue()

    # Sorting the results
    sorted_results = OrderedDict(sorted(result.items()))

    # Displaying the results
    for key, value in sorted_results.items():
        print(key, ' ', value)


if __name__ == '__main__':
    main()
