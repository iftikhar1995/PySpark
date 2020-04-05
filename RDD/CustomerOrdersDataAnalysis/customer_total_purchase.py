from pyspark import SparkConf
from pyspark import SparkContext


def customer_record_parser(record: str) -> tuple:
    """
    The function will parse the records of customer.

    :param record: A line from the data file
    :type record: str
    :return: The customer id and purchase amount
    """

    # Getting the fields from the records
    fields = record.split(',')

    # Extracting the id of customer and converting it into desired format.
    customer_id = int(fields[0])

    # Extracting the purchase amount of customer and converting it into desired format.
    purchase = float(fields[2])

    return customer_id, purchase


def main():
    """
    The driver function. It is responsible for applying all transformation and will print the results onto the console.

    :return:
    """

    # Setting the job configurations
    conf = SparkConf().setMaster('local').setAppName('CustomerTotalPurchases')

    # Creating the entry point to spark functionality
    sc = SparkContext(conf=conf)

    # Extracting data from the data file into RDD
    data_rdd = sc.textFile('../../Resources/CustomerOrders/customer-orders.csv')

    # First the customer data is parsed and we extract customer id and amount he/she
    # spent on purchasing. After that for each customer id we are calculating the total
    # amount of purchases a customer has made. Then we transform the customer id, purchases
    # rdd into purchases and customer id rdd and finally sort the results based on the
    # amount a customer has spent
    total_purchase_rdd = data_rdd.map(customer_record_parser)\
                                 .reduceByKey(lambda x, y: x+y)\
                                 .map(lambda x: (x[1], x[0]))\
                                 .sortByKey()

    # Extracting the results
    results = total_purchase_rdd.collect()

    # Displaying the results on console
    for amount, _id in results:
        print(_id, ' {:.2f}'.format(amount))


if __name__ == '__main__':
    main()
