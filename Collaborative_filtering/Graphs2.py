
# coding: utf-8

# In[3]:

import pandas as pd
import numpy as np


# In[4]:

with open('year1_test_triplets_visible.txt') as f:
    table = pd.read_table(f, sep='\t', header=None,
                          lineterminator='\n')


# In[5]:

table.columns = ['user', 'song', 'count']


# In[6]:

print len(table.user.unique())


# In[14]:

temp = table.groupby("user").agg({"count": np.sum, "song": pd.Series.nunique})
temp = temp.div(temp.song, axis = 0).sort("count")
print temp


# In[22]:

ax = temp[['count']].plot()
ax.format_coord = lambda x, y: ''
print ax


# In[23]:

import matplotlib.pyplot as plt
ax.axes.get_xaxis().set_ticks([])
ax.set_xlabel("USER", fontsize=12)
ax.set_ylabel("Frequency per song", fontsize=12)
plt.show()

