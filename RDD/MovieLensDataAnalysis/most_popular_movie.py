from pyspark import SparkConf
from pyspark import SparkContext


def main():
    """
    The driver function. It is responsible for applying all transformation and will print the results onto the console.

    :return:
    """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('MostPopularMovie')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/MovieLens/u.data')

    # We start with extraction of movie id, then we create a tuple like (movie_id, 1).
    # After that we calculate the frequency of the movies by summing up all the ones.
    # Then we toggle the tuple value - from movie_id, frequency to frequency, movie_id
    # and finally sort the results on the movie id
    watched_frequency = data_rdd.map(lambda line: (int(line.split()[1]), 1))\
                                .reduceByKey(lambda x, y: x + y)\
                                .map(lambda x: (x[1], x[0]))\
                                .sortByKey()

    # Getting the results
    results = watched_frequency.collect()

    # Displaying them into console
    for frequency, movie_id in results:
        print(movie_id, '  ', frequency)


if __name__ == '__main__':
    main()
