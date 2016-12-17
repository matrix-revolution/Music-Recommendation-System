
# coding: utf-8

# In[4]:

import pandas as pd
import numpy as np


# In[5]:

with open('year1_test_triplets_visible.txt') as f:
    table = pd.read_table(f, sep='\t', header=None,
                          lineterminator='\n')


# In[6]:

table.columns = ['user', 'song', 'count']


# In[7]:

print len(table.user.unique())


# In[8]:

temp = table.groupby("user").agg({"count": np.sum, "song": pd.Series.nunique}).sort("song")
print temp


# In[9]:

ax = temp[['song']].plot()
ax.format_coord = lambda x, y: ''
print ax


# In[10]:

import matplotlib.pyplot as plt
ax.axes.get_xaxis().set_ticks([])
ax.set_xlabel("USER", fontsize=12)
ax.set_ylabel("Unique Songs", fontsize=12)
plt.show()

