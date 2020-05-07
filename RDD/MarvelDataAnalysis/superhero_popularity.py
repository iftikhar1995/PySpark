from pyspark import SparkConf
from pyspark import SparkContext


def hero_information_parser(record: str) -> tuple:
    fields = record.split('\"')
    hero_id = int(fields[0])
    hero_name = fields[1].encode('utf8')

    return hero_id, hero_name


def hero_links_parser(record: str) -> tuple:
    links = record.split()
    return int(links[0]), len(links) - 1


def main():
    conf = SparkConf().setMaster('local').setAppName('SuperHeroPopularity')
    sc = SparkContext(conf=conf)

    hero_info_rdd = sc.textFile('../../Resources/Marvel/MarvelNames.txt')\
                      .map(hero_information_parser)

    hero_links_rdd = sc.textFile('../../Resources/Marvel/MarvelGraph.txt')\
                       .map(hero_links_parser)\
                       .reduceByKey(lambda x, y: x + y)\
                       .map(lambda x: (x[1], x[0]))\
                       .max()

    most_popular_name = hero_info_rdd.lookup(key=hero_links_rdd[1])[0]

    print(most_popular_name)


if __name__ == '__main__':
    main()
