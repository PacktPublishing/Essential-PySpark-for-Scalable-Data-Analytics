# Databricks notebook source
# MAGIC %md
# MAGIC ##Visualizations using Python visualization libraries.
# MAGIC Various types of charts and graphs are avaibale using prominent Python visualizxation libraries.
# MAGIC ###1. Matplotlib

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

retail_df = spark.read.table("feature_store.retail_features")
viz_df = retail_df.select("cust_age", "work_type", "fin_wt", "invoice_month", "country_code", "quantity", "unit_price", "occupation", "gender")
pdf = viz_df.toPandas()

pdf['quantity'] = pd.to_numeric(pdf['quantity'], errors='coerce')
pdf.plot(kind='bar', x='invoice_month', y='quantity', color='orange')

display()

# COMMAND ----------

# MAGIC %md ###2. Seaborn

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

data = np.random.normal(10, 15, 20)
data = viz_df.select("unit_price").toPandas()["unit_price"]

plt.figure(figsize=(10, 3))
sns.boxplot(data)

# COMMAND ----------

# MAGIC %md ###2. Plotly

# COMMAND ----------

from plotly.offline import plot
from plotly.graph_objs import *
import numpy as np

#x = np.random.randn(2000)
#y = np.random.randn(2000)

x = viz_df.select("cust_age").toPandas()
y = viz_df.select("quantity").toPandas()

p = plot(
  [
    Histogram2dContour(x=x, y=y, contours=Contours(coloring='heatmap')),
    Scatter(x=x, y=y, mode='markers', marker=Marker(color='white', size=10, opacity=0.1))
  ],
  output_type='div'
)

displayHTML(p)

# COMMAND ----------

import plotly.express as plot
df = viz_df.toPandas()
fig = plot.scatter(df, x="fin_wt", y="quantity",
	         size="unit_price", color="occupation",
                 hover_name="country_code", log_x=True, size_max=60)
fig.show()

# COMMAND ----------

# MAGIC %md ###3. Altair

# COMMAND ----------

# MAGIC %pip install altair

# COMMAND ----------

import altair as alt
import pandas as pd

source = (viz_df
        .selectExpr("gender as Gender", "trim(occupation) as Occupation")
        .where("trim(occupation) in ('Farming-fishing', 'Handlers-cleaners', 'Prof-specialty', 'Sales', 'Tech-support') and cust_age > 49 ")
        .toPandas())

alt.Chart(source).mark_text(size=45, baseline='middle').encode(
    alt.X('x:O', axis=None),
    alt.Y('Occupation:O', axis=None),
    alt.Row('Gender:N', header=alt.Header(title='')),
    alt.Text('emoji:N')
).transform_calculate(
    emoji="{'Farming-fishing': '🌾', 'Handlers-cleaners': '🔧', 'Prof-specialty': '💼', 'Sales': '🔬', 'Tech-support':'💻'}[datum.Occupation]"
).transform_window(
    x='rank()',
    groupby=['Gender', 'Occupation']
).properties(width=600, height=200)