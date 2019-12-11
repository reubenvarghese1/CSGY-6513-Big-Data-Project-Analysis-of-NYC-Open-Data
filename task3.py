import pandas as pd 
import matplotlib.pyplot as plt

path = "data.csv"
df = pd.read_csv(path)
df.head()

df = pd.read_csv('data.csv', index_col='Unique Key')

df.head(3)
type(df)

boroughcount = df.groupby(['Complaint Type','Borough']).size().unstack()

figures, axes = plt.subplots(3,2, figsize=(5,12))

for x, (label,col) in enumerate(boroughcount.iteritems()):
    axis = axes[int(x/2), x%2]
    col = col.sort_values(ascending=True)[:3]
    col.plot(kind='bar', ax=axis)
    axis.set_title(label)
    
plt.tight_layout()