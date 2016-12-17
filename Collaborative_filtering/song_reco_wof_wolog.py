from pyspark.sql import SparkSession
from operator import add
import sys
import re
import itertools
import string
import math
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import SparseVector

from pyspark.sql import SparkSession
from operator import add
import sys
import re
import itertools
import string
debug = False

K_neigh = 50
User_threshold = 5
Song_threshold = 10

base = "./data/"
file_name  = "year1_test_triplets_visible.txt"
#file_name = "ratings_Video_Games.csv.gz"
#item_id = u'B000035Y4P'

#file_name = "ratings_Electronics.csv.gz"
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
user_to_item = user_to_item.map(lambda x: (x[0], (x[1][0], x[1][1])))

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


base = "./data/"
file_name  = "year1_test_triplets_visible.txt"

# base = "./ratings/"
# file_name = "ratings_Video_Games.csv.gz"
# file_name = "short.csv"

support = 15
confidence = 0.04
interest = 0.01
MAX_K = 4

def association_rule_mining(baskets):
    #print "XXX Association Rules:"
    #print "XXX #Baskets: "
    #print baskets.count()

    def mapper(kv):
        itemset = kv[0]
        value = kv[1]

        ret = list()

        item4 = set(itemset)
        for item in item4:
            item3 = tuple(sorted(list(item4 - set((item,)))))
            item = (item,)

            c =  float(value) / float(frequent_sets[3][item3])
            i = c -  (float(frequent_sets[1][item]) / float(num_baskets))

            if c >= confidence and i >= interest:
                ret.append((item3, item, c, i))

        return ret

    rules = frequent_sets_rdd[4].flatMap(mapper)
    rules_list = rules.collect()
    associations = sorted(rules_list, key=lambda x: x[2])
    for (I, j, c, i) in associations:
        print I, "=>", j, "confidence:", c, "interest:", i
    return associations


def apriori(baskets):
    # baskets.cache()
    global frequent_sets
    items = baskets.flatMap(lambda x: x).distinct()
    frequent_sets[0] = items.map(lambda x: ((x,),1)).collectAsMap()
    # print frequent_sets[0]
    frequent_items = None
    def spit_subsets(basket, size):
        ret = list()

        subset_size = 1 if (size == 1) else size - 1

        for candidate in itertools.combinations(basket, size):
            candidate = tuple(sorted(candidate))

            good = True

            for subset in itertools.combinations(candidate, subset_size):
                subset = tuple(sorted(subset))

                if subset not in frequent_sets[size - 1]:
                    good = False
                    break

            if good:
                ret.append((candidate, 1))

        return ret

    def filter_non_freq_item(basket):
        if frequent_items == None:
            return basket

        all_items = set(basket)
        remove_items = set()

        for candidate in basket:
            candidate = (candidate,)
            if candidate not in frequent_items:
                remove_items |= set(candidate)
        # print remove_items
        items = all_items - remove_items
        return list(items)

    print "INIT:" , baskets.flatMap(lambda x: x).count()
    for size in xrange(1, MAX_K+1):
        print "BASKET NOW:" , baskets.map(filter_non_freq_item).flatMap(lambda x: x).count()
        CK = baskets  \
                .map(filter_non_freq_item) \
                .flatMap(lambda x: spit_subsets(x, size)).reduceByKey(add) \
                .filter(lambda candidate: candidate[1] >= support)
        frequent_sets_rdd[size] = CK

        frequent_sets[size] = CK.collectAsMap()

        x = set(CK.keys().flatMap(lambda x: [(item,) for item in x]).distinct().collect())

        if frequent_items == None: frequent_items = x
        else: frequent_items &= x

        print "Frequent_items len: ", len(frequent_items)
        print "Freqeunt_sets of size %d : %d" % (size, len(frequent_sets[size]))


lines = sc.textFile(base + file_name)
print "XXX Total Lines: ", lines.count()

splits = lines.map(lambda lines: tuple(lines.split()[:2]))


# Using only distinct pairs
# splits = splits.distinct()
print "XXX Distinct pairs: "
print splits.count()

# Item to user mapping
# filter to items associated with at least 10 users

reverse = splits.map(lambda x: (x[1], x[0]))
item_mapping = reverse.groupByKey().mapValues(list)
filtered_items = item_mapping.filter(lambda x: len(x[1]) >= Song_threshold)
print "XXX Items having threshold: "
print filtered_items.count()

# filtering users having minimum 5 items
user_item = filtered_items.flatMap(lambda x: [(user, x[0]) for user in x[1] ])
filtered_users = user_item.groupByKey().mapValues(lambda v: sorted(v)).filter(lambda x: len(x[1]) >= User_threshold)
print "XXX Users having threshold:", filtered_users.count()
frequent_items = None
frequent_sets = [None for x in range(MAX_K + 1)]
frequent_sets_rdd = [0] * 5
baskets = filtered_users.values()
num_baskets = baskets.count()
print "#Baskets: ", num_baskets

apriori(baskets)
associations = association_rule_mining(baskets)

associated_items = set()
associated_dic = {}
for i, j, k, l in associations:
    associated_items |= set(i)
    associated_items |= set(j)
    associated_dic[i] = j

print list(associated_items)

def filter_non_associated_item(user_basket):
        user = user_basket[0]
        basket = user_basket[1]
        all_items = set(basket)
        remove_items = set()

        for candidate in basket:
            if candidate not in associated_items:
                remove_items |= set(candidate)
        # print remove_items
        items = all_items - remove_items
        return (user, list(items))

def get_recommendation(user_basket):
        user = user_basket[0]
        basket = user_basket[1]
        reco = set()
        for candidate in itertools.combinations(basket, 3):
            candidate = tuple(sorted(candidate))
            try:
                item = associated_dic[candidate]
                if item not in basket:
                    reco.add(item)
            except:
                pass
        return (user, tuple(sorted(list(reco))))

can_get_recos = filtered_users.map(filter_non_associated_item).filter(lambda x: len(x[1]) > 0)
# print "#Users for which recos can be get:", can_get_recos.count()
recommendations = can_get_recos.map(get_recommendation).filter(lambda x: len(x[1]) > 0).collectAsMap()


reco_list = list()

try:
    rr = None
    if user_id in recommendations:
        rr = recommendations[user_id]
    elif (user_id,) in recommendations:
        rr = recommendations[(user_id,)]
    for song in rr:
        if song[0] not in reco_list:
            print "Associate rule", song[0]
            reco_list.append(song[0])
except:
    print "No rule present for user_id", user_id
    pass

for k,v in sorted(prediction.items(), key=lambda x:x[1], reverse=True):
    print user_id,"\t", k ,"\t", v
    if len(reco_list) < 500:
        if k not in reco_list:
            reco_list.append(k)

"""
for user, recos in recommendations:
    print user, "=>", recos
"""

print "Recommendations coming:"
for song in reco_list:
    print song

hidden_lines = sc.textFile(base + 'year1_test_triplets_hidden.txt')
splits = hidden_lines.map(lambda line: tuple(line.split()[:3]))
splits = splits.filter(lambda x: x[0] == user_id)
songs_hidden  = splits.collect()
hidden_list = list()
for h in songs_hidden:
    hidden_list.append(h[1])

print "Hidden List: ", hidden_list
print "Len of reco_list: ", len(reco_list)
print "Len of hidden List:", len(hidden_list)
hit = set(reco_list) & set(hidden_list)
print "Hit list:", list(hit)
precision = float(len(hit)) / float(len(reco_list))
print "Precision:", precision
sc.stop()

