import pyspark as py
conf=py.SparkConf().setAppName("dayet")
# sc=py.SQLContext(conf=conf)
sc=py.SparkContext(conf=conf)
hsc=py.HiveContext(sc)
def sql():
    hsc.sql("SELECT  *  FROM datas ORDER BY create_time DESC").show()
if __name__ == '__main__':
    sql()