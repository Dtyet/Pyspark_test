import pyspark as ps
if __name__ == '__main__':
    conf=ps.SparkConf().setAppName("aaa").setMaster("local[1]")
    sc=ps.SparkContext(conf=conf)
    lines=sc.textFile("D:\\Users\\11.txt")
    words=lines.flatMap(lambda line: line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:(a+b))
    # sc.addFile()
    a=lines.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).countByKey()
    # print(a)
    # print(words.first())

    words.sortByKey().foreach(print)
