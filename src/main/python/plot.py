import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import json

infiles = {
    "src/main/resources/canonical-edges-s3-aggregations-cc-2015-11.json": "CommonCrawl 2015/11",
    "src/main/resources/canonical-edges-s3-aggregations-cc-2017-04.json": "CommonCrawl 2017/04",
    "src/main/resources/canonical-edges-s3-aggregations-cw09.json": "Clueweb 09",
    "src/main/resources/canonical-edges-s3-aggregations-cw12.json": "Clueweb 12"
}
    
df = []
for path, corpus in infiles.items():
    with open(path, 'r') as file:
        data = (
            pd.DataFrame(pd.Series(json.load(file)))
            .reset_index()
            .rename({'index': 'X', 0: 'Y'}, axis = 1)
        )
        data['Corpus'] = corpus
        df.append(data)
df = pd.concat(df)
df.X = df.X.astype(float)
df.Y = df.Y.astype(float)

sns.set_style({
     'axes.facecolor': 'white',
     'axes.edgecolor': '.05',
     'axes.grid': True,
     'axes.axisbelow': True,
     'axes.labelcolor': '.0',
     'figure.facecolor': 'white',
     'grid.color': '.75',
     'grid.linestyle': '-',
     'text.color': '.15',
     'xtick.color': '.05',
     'ytick.color': '.05',
     'xtick.direction': 'in',
     'ytick.direction': 'in',
     'patch.edgecolor': 'w',
     'patch.force_edgecolor': True,
     'font.family': ['sans-serif'],
     'font.sans-serif': ['Arial', 'Helvetica Neue', 'sans-serif'],
     'xtick.bottom': False,
     'xtick.top': False,
     'ytick.left': False,
     'ytick.right': False,
     'axes.spines.left': False,
     'axes.spines.bottom': True,
     'axes.spines.right': False,
     'axes.spines.top': False
})
g = sns.FacetGrid(
    df, 
    col = "Corpus", 
    col_order = ["Clueweb 09", "Clueweb 12", "CommonCrawl 2015/11", "CommonCrawl 2017/04"]
)

g = (
    g.map(
        plt.plot, 
        "X", 
        "Y", 
        color = 'k')
    .set(yscale = 'log')
)
sns.despine(left=True)
plt.tight_layout()
g.savefig('canonical-edges.pdf')