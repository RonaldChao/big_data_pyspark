from pyspark import SparkConf, SparkContext
import collections

# conf = SparkConf.setMaster("local[*]").setAppName("RatingsCount")
sc = SparkContext.getOrCreate(SparkConf().setAppName('Ratings'))

lines = sc.textFile('../dataset/ml-latest-small/ratings.csv')
ratings = lines.map(lambda line: line.split(',')[2])
results = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))