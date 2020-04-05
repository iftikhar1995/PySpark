from pyspark import SparkConf
from pyspark import SparkContext


def movie_names():
    movies = dict()

    with open('../../Resources/MovieLens/u.item') as data:
        for record in data:
            fields = record.split('|')
            movie_id = int(fields[0])
            movie_name = fields[1]
            movies[movie_id] = movie_name

    print(movies)
    return movies


def main():
    """
        The driver function. It is responsible for applying all transformation and will print the results onto the console.

        :return:
        """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('MostPopularMovie')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    movies = sc.broadcast(movie_names())

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/MovieLens/u.data')

    # We start with extraction of movie id, then we create a tuple like (movie_id, 1).
    # After that we calculate the frequency of the movies by summing up all the ones.
    # Then we toggle the tuple value - from movie_id, frequency to frequency, movie_id
    # and finally sort the results on the movie id
    watched_frequency = data_rdd.map(lambda line: (int(line.split()[1]), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey()

    sorted_movies_with_names = watched_frequency.map(lambda x: (movies.value[x[1]], x[0]))

    # Getting the results
    results = sorted_movies_with_names.collect()

    # Displaying them into console
    for result in results:
        print(result)


if __name__ == '__main__':
    main()
