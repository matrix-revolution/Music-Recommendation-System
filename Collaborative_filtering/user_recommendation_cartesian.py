from pyspark.sql import SparkSession
from operator import add
import sys
import re
import itertools
import string
import math
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import SparseVector

debug = False

K_neigh = 50
User_threshold = 5
Song_threshold = 10

base = "./data/"
file_name  = "year1_test_triplets_visible.txt"
#file_name = "ratings_Video_Games.csv.gz"
#item_id = u'B000035Y4P'

#file_name = "ratings_Electronics.csv.gz"
item_id = 'SOXNWYP12A6D4FBDC4'
user_id = '7d90be8dfdbde170f036ce8a4b915440137cb11c'
def normalize(values):
    s = 0.0
    for user,rating in values:
        s += rating
    mean = s / float(len(values))
    new_values = []
    for user, rating in values:
        new_values.append((user, rating - mean))
    return new_values

## find neighbourhood for itemx
def predict(kv):
    rating = 0.0
    deno = 0.0
    neno = 0.0

    ## Filter 50 best neighbours
    values = list(sorted(kv[1], key=lambda x: x[1], reverse=True))
    for item, sim, r in values[:K_neigh]:
        neno += r * sim
        deno += sim
    rating = neno / deno

    return (kv[0], rating)


def cosine(kv):
        item1 = kv[0][0]
        row1 = kv[0][1]
        item2 = kv[1][0]
        row2 = kv[1][1]

        dicrow2 = {}
        num = float(0.0)
        modA = modB = 0

        for (user, r) in row2:
            modB += r * r
            dicrow2[user] = r

        for (user, r) in row1:
            try:
                modA += r * r
                num += r * dicrow2[user]
            except:
                ## if doesn't find user in dicrow2
                pass
        modB = modB ** (0.5)
        modA = modA ** (0.5)

        if modA == 0 or modB == 0:
            return (item1, (item2, -1, myuser_ratings[item2]))

        cos = num / (modA * modB)

        return (item1, (item2, cos, myuser_ratings[item2]))


spark = SparkSession\
            .builder\
            .appName("Collab-filtering")\
            .getOrCreate()
sc = spark.sparkContext

lines = sc.textFile(base + file_name)
print "Total Lines: ", lines.count()

latest_ratings = lines.map(lambda line: line.split('\t')).map(lambda x: (x[1], (x[0], float(x[2]))))  # item => user, rating

# filter to songs associated with at least 25 distinct users
item_mapping = latest_ratings.groupByKey().mapValues(list)

# Song to user mapping
filtered_items = item_mapping.filter(lambda x: len(x[1]) >= Song_threshold) # 25
print "Songs having threshold: ", filtered_items.count()

# filtering users having minimum 10 songs
user_to_item = filtered_items.flatMap(lambda x: [(user, (x[0], rating)) for user,rating in x[1]])

## converting listenning count to Log-scale
user_to_item = user_to_item.map(lambda x: (x[0], (x[1][0], min(5.0, math.log(x[1][1], 2) + 1.0))))

filtered_user_to_item = user_to_item.groupByKey().mapValues(lambda v: sorted(v)).filter(lambda x: len(x[1]) >= User_threshold) #10  # (user) => [(item, rating) , ()]
print "Users having threshold:", filtered_user_to_item.count()

"""
# indexing users
user_idx = {}
for idx, uid in enumerate(list(sorted(filtered_user_to_item.keys().collect()))):
   user_idx[uid] = idx
print "Length of user_idx:", len(user_idx)
"""

item_to_user = filtered_user_to_item.flatMap(lambda x: [(item, (x[0], rating) ) for item, rating in x[1]])
utility_matrix = item_to_user.groupByKey()

utility_matrix_norm = utility_matrix.mapValues(normalize)
myuser_ratings = dict(filtered_user_to_item.lookup(user_id)[0])

## filter out only those items rating by this user
sub_utility_matrix = utility_matrix_norm.filter(lambda x: x[0] in myuser_ratings) ## item rated by user

utility_matrix_norm = utility_matrix_norm.filter(lambda x: x[0] not in myuser_ratings)
### Cartesian product

print "sub_utility_matrix len", sub_utility_matrix.count()
print "utility_matrix len", utility_matrix_norm.count()

prediction = utility_matrix_norm.cartesian(sub_utility_matrix)\
        .map(cosine)\
        .filter(lambda x: x[1][1] >  0)\
        .groupByKey().mapValues(list)\
        .filter(lambda x: len(x[1]) > 5)\
        .map(predict).collectAsMap()

for k,v in sorted(prediction.items(), key=lambda x:x[1], reverse=True):
    print user_id,"\t", k ,"\t", v

"""
### TEMPORARY CODE BEGINS - DO NOT LOOK HERE
### COMMENTED OUT ## =============
###

## similar_matrix = cosine_similarity.filter(lambda x: x[1] > 0).sortByKey()

    # predicted_ratings = user_item.map(predict).map(lambda x: (x[0], x[1] + mean_rating)).collect()
    predicted_ratings = user_item.map(predict).collectAsMap()


# original_item_row = utility_matrix.lookup(item_id)[0]
# print utility_matrix.collect()

def get_ratings(userx):
    print "Getting rating"
    # allusers = filtered_user_to_item
    user_col = filtered_user_to_item.lookup(user_id)[0]



    row = utility_matrix_norm.lookup(itemx)[0]
    # print row

    def mymapper(kv):
        item = kv[0]
        user_rating = kv[1][0][0]
        sim = kv[1][1][0]
        return [(user, (item, r, sim)) for (user, r) in user_rating]

    similar_items = cosine_similarity.filter(lambda x: x[1] > 0).sortByKey()
    # print similar_items.collect()

    ## filtering out the items from utility which has negative similarity
    unwanted = utility_matrix.subtractByKey(similar_items)

    ## This matrix has similar items
    similar_matx = utility_matrix.subtractByKey(unwanted)

    ## Attach similarty in each item
    mtx = similar_matx.cogroup(similar_items).mapValues(lambda x: [list(y) for y in x])

    ## reverse the matrix for each user
    user_item = mtx.flatMap(mymapper).groupByKey().mapValues(list) # (user) => [(item, rating) , ()]

    ## filter users who didn't rate atleast two similar items
    user_item = user_item.filter(lambda x: len(x[1]) > 1)

    get_ratings(user_idx[user_id])
"""
sc.stop()

