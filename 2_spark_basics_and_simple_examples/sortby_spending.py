from pyspark import SparkContext, SparkConf

sc = SparkContext.getOrCreate(conf = SparkConf())

# Read in the customer spending dataset
# Customer ID, item ID, spending
data = sc.textFile('../dataset/customer-orders.csv')

# Split the fields with comma
lines = data.map(lambda x: x.split(','))

# Extract the customer ID and his/her spendings
customerID_spending = lines.map(lambda x: (int(x[0]), float(x[2])))

# Sum up the spending by customer ID
# Swap the spending and customer ID
total_spending_per_customer = customerID_spending.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0]))

# Sort by spending 
total_spending_per_customer_sorted = total_spending_per_customer.sortByKey()

# Generate an array
results = total_spending_per_customer_sorted.collect()

for result in results:
    print(str(result[1]) + ':\t\t' + '{0:.2f}'.format(result[0]))