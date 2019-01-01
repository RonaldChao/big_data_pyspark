from pyspark import SparkConf, SparkContext

sc = SparkContext.getOrCreate(SparkConf())

def parse_line(line):
    fields = line.split(',')
    loc_id = fields[0]
    symbol = fields[2]
    temp = int(fields[3])

    return (loc_id, symbol, temp)

# Read in the file
rdd = sc.textFile('../dataset/1800.csv')

# formatted field (id, symbol, temperature)
extracted_rdd = rdd.map(parse_line)

# Only extract the TMIN
tmin_rdd = extracted_rdd.filter(lambda x: 'TMIN' in x[1])

# reduce it further to (id, temperature)
loc_id_temp = tmin_rdd.map(lambda x: (x[0], x[2]))

# Find the mininum temp of a location
min_temp_loc = loc_id_temp.reduceByKey(lambda x, y: min(x,y))

results = min_temp_loc.collect()

print('---------------------------------')

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

print('---------------------------------')