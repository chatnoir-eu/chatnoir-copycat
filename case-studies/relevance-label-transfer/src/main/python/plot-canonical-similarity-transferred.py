import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import json

infiles = {
    "src/main/resources/s3-aggregations-cw09-to-cw12.json": "CW 09 to CW 12",
    "src/main/resources/s3-aggregations-cw09-to-cw12-and-wayback.json": "CW 09 to CW 12 + Wayback",
    "src/main/resources/s3-aggregations-cw-to-cc15.json": "CWs to CC 15",
}

df = []
for path, corpus in infiles.items():
    with open(path, 'r') as file:
        data = (
            pd.DataFrame(pd.Series(json.load(file)))
                .reset_index()
                .rename({'index': 'S3 Score', 0: 'Number'}, axis=1)
        )
        data['Corpus'] = corpus
        df.append(data)
df = pd.concat(df)
df['S3 Score'] = df['S3 Score'].astype(float)
df['Number'] = df['Number'].astype(float)
df = df.sort_values(['Corpus', 'S3 Score'])

df = (
    df
    .merge(
        df
        .groupby('Corpus')
        .sum()
        .rename({'Number': 'Sum'}, axis=1)
        .drop('S3 Score', axis=1),
        on='Corpus',
        how='left'
    )
)

print(df)

df['Cumulative'] = df.loc[:, ['Number', 'Corpus']].groupby('Corpus').cumsum()
df['Ratio'] = df['Cumulative'] / df['Sum']
df = df.rename({'S3 Score': '$S_{3}$ Score'}, axis=1)

print(df)

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

kw = {'color': ['k', 'k', 'k'], 'linestyle': ["-", "--", ":"]}

g = sns.lineplot(data=df, x='$S_{3}$ Score', y='Ratio', style="Corpus", color='k', style_order=["CW 09 to CW 12", "CW 09 to CW 12 + Wayback", "CWs to CC 15"])

g.legend(bbox_to_anchor=(.515, .9), loc=1, borderaxespad=0.)
sns.despine(left=True)
plt.tight_layout()
g.get_figure().savefig('canonical-edges-transferred.pdf')
