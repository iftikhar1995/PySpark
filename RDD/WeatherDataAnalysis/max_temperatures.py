from pyspark import SparkConf
from pyspark import SparkContext


def parse_weather_record(record: str) -> tuple:
    """
    The function will parse the records specified in data file.

    :param record: A record from  data file
    :type record: str
    :return: The id of station, type of temperature i.e. TMIN, TMAX, and the temperature value
    """

    # Separating each field.
    fields = record.split(',')

    # Extracting the id of the station.
    station_id = fields[0]

    # Extracting the temperature's type
    _type = fields[2]

    # Extracting the temperatures and converting it into desired format
    temperatures = float(fields[3])

    return station_id, _type, temperatures


def main():
    """
    The driver function. It is responsible for applying all transformation and will print the results onto the console.

    :return:
    """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('MaxTemperatures')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/Weather/1800.csv')

    # Parsing the data
    parsed_data = data_rdd.map(parse_weather_record)

    # Performing the transformations. First it will filter those records having
    # the temperatures type TMAX, then transform rdd into station id and temperature,
    # and finally it finds the maximum temperature that is recorded against the station
    # up till now.
    min_temperatures = parsed_data.filter(lambda record: 'TMAX' == record[1]) \
        .map(lambda record: (record[0], record[2])) \
        .reduceByKey(lambda x, y: min(x, y))

    # Getting the results
    results = min_temperatures.collect()

    # Displaying them into console
    for result in results:
        print(result[0], '{:.2f}C'.format(result[1]))


if __name__ == '__main__':
    main()
