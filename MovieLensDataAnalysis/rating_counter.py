from pyspark import SparkConf, SparkContext
from collections import OrderedDict


def main():
    # Preparing the configuration for spark
    conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')

    # Creating a spark context.
    sc = SparkContext(conf=conf)

    # Setting the log level to ERROR
    sc.setLogLevel(logLevel='ERROR')

    # Reading the data file in RDD
    data = sc.textFile("../Resources/MovieLens/u.data")

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
