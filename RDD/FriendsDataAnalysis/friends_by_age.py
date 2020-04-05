from pyspark import SparkConf
from pyspark import SparkContext


def parse_friend_record(record: str) -> tuple:
    """
    The function will parse a single line from fakefriends.csv file.

    :param record: A single line from the data file.
    :type record: str
    :return: the age and num of friends from the data file.
    :rtype: tuple
    """

    # Separating each field.
    data = record.split(',')

    # Extracting the age and converting it into desired format
    age = int(data[2])

    # Extracting the number of friends and converting it into desired format
    num_friends = int(data[3])

    return age, num_friends


def main():
    """
    The driver function. It is responsible for applying all transformation and will print the results onto the console.

    :return:
    """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('FriendsByAge')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/FakeFriends/fakefriends.csv')

    # Getting age and number of friends, that age fellow have, into RDD.
    age_friends_rdd = data_rdd.map(parse_friend_record)

    # Calculating the total number of friends a person of specific age have.
    totals_by_age = age_friends_rdd.mapValues(lambda num_friends: (num_friends, 1))\
                                   .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # Calculating the average number of the friends a person of specific age have.
    average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])

    # Getting the results
    results = average_by_age.collect()

    # Displaying the results onto the console
    for result in results:
        print(result)


if __name__ == '__main__':
    main()
