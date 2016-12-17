
# coding: utf-8

# In[2]:

import pandas as pd
import numpy as np


# In[3]:

with open('year1_test_triplets_visible.txt') as f:
    table = pd.read_table(f, sep='\t', header=None,
                          lineterminator='\n')


# In[4]:

table.columns = ['user', 'song', 'count']


# In[5]:

print len(table.user.unique())


# In[58]:

temp = table.groupby("song").agg({"count": np.sum, "user": pd.Series.nunique}).sort("user", ascending = False)
print len(temp)

temp = temp[temp['user'] > 900]
print len(temp)


print temp
#temp = temp[temp['user'] > 1000]
#print len(temp)


# In[46]:

ax = temp[['user']]
ax.format_coord = lambda x, y: ''
graph = ax.plot()
print graph


# In[47]:

import matplotlib.pyplot as plt

#graph.axes.get_xaxis().set_visible(False)
graph.axes.get_xaxis().set_ticks([])
graph.set_xlabel("Songs", fontsize=12)
graph.set_ylabel("Unique Users", fontsize=12)
plt.show()

