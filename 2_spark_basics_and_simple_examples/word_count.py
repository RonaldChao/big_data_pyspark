from pyspark import SparkConf, SparkContext
import re
import collections

sc = SparkContext.getOrCreate(SparkConf())

# Split by non-character words
def split_words(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())

# Read in the text file
text = sc.textFile('../dataset/Book.txt')

# Transform to one word per row
words = text.flatMap(split_words)

# Generate (word, count) pair
word_count = words.countByValue()

# Sort the (word, count) pair in ascending order
sortedResults = collections.OrderedDict(sorted(word_count.items(), key=lambda x: x[1]))

# print out the word and its corresponding count
for word, count in sortedResults.items():
    # check if the word is empty
    if (word):
        print(word + ':\t\t' + str(count))
