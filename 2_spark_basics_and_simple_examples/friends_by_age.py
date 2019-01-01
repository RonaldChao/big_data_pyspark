from pyspark import SparkContext, SparkConf

sc = SparkContext.getOrCreate(SparkConf())

# Parse the line and form key-value pair like (age, friend count)
def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    friends_count = int(fields[3])

    return (age, friends_count)

# readin the csv
lines = sc.textFile('../dataset/fakefriends.csv')
rdd = lines.map(parse_line)

# generate key-value pair for averaging later (age, (friend count, 1))
rdd2 = rdd.mapValues(lambda x: (x,1))

# generate the key-value pair (age, (total friend, total count))
total_by_age = rdd2.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

# calculate the average of the fiend count
average_friend_count = total_by_age.mapValues(lambda x: x[0]/x[1])
average_friend_count_sorted = average_friend_count.sortByKey()

results = average_friend_count_sorted.collect()

for result in results:
    print(result)

