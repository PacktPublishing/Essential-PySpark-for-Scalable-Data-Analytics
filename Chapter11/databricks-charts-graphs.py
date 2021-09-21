# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##PySpark Native visualizations using Databricks Notebooks
# MAGIC Various types of charts and graphs are avaibale using Databricks notebooks built-in `display()` function.

# COMMAND ----------

# MAGIC %md  ### A basic **tabular form** of data.
# MAGIC Only the first 1000 rows are displayed in the table view.<br>
# MAGIC *Dataset created towards the end of Chapter 6, Feature Engineering*

# COMMAND ----------

retail_df = spark.read.table("feature_store.retail_features")
viz_df = retail_df.select("invoice_num", "description", "invoice_date", "invoice_month", "country_code", "quantity", "unit_price", "occupation", "gender")
viz_df.display()

# COMMAND ----------

# MAGIC %md  ### Pivot Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To create a Pivot table, click the Graph icon below a result and select **Pivot**.<br>
# MAGIC Click on plot options to choose the, key, value and series groupings.

# COMMAND ----------

viz_df.display()

# COMMAND ----------

# MAGIC %md ### Bar Chart

# COMMAND ----------

viz_df.display()

# COMMAND ----------

# MAGIC %md **Tip:** Hover over each bar in the chart below to see the exact values plotted.

# COMMAND ----------

# MAGIC %md ### Line Graph

# COMMAND ----------

# MAGIC %sql select * from feature_store.retail_features

# COMMAND ----------

# MAGIC %md ### Pie Chart

# COMMAND ----------

# MAGIC %sql select * from feature_store.retail_features

# COMMAND ----------

# MAGIC %md ### Geo Map

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   case
# MAGIC     when country_code = 'EN' then 'GBR'
# MAGIC     else 'FRA'
# MAGIC   end as country_code,
# MAGIC   gender,
# MAGIC   unit_price
# MAGIC from
# MAGIC   feature_store.retail_features

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To plot a graph of the world, use [country codes in ISO 3166-1 alpha-3 format](http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) as the key.

# COMMAND ----------

# MAGIC %md ### Scatter Plot

# COMMAND ----------

# MAGIC %sql select * from feature_store.retail_features

# COMMAND ----------

# MAGIC %md ### Histogram

# COMMAND ----------

# MAGIC %sql select * from feature_store.retail_features

# COMMAND ----------

# MAGIC %md ### A **Quantile plot** allows you to view what the value is for a given quantile value.
# MAGIC * For more information on Quantile Plots, see http://en.wikipedia.org/wiki/Normal_probability_plot.
# MAGIC * **Plot Options...** was selected to configure the graph below.
# MAGIC * **Value** should contain exactly one field.
# MAGIC * **Series Grouping** is always ignored.
# MAGIC * **Keys** can support up to 2 fields.
# MAGIC   * When no key is specified, exactly one quantile plot is output.
# MAGIC   * When 2 fields are specified, then there is a trellis of quantile plots .
# MAGIC * **Aggregation** is not applicable.
# MAGIC * Quantiles are not being calculated on the serverside for now, so only the 1000 rows can be reflected in the plot.

# COMMAND ----------

# MAGIC %sql select * from feature_store.retail_features

# COMMAND ----------

# MAGIC %md ### A **Q-Q plot** shows you how a field of values are distributed.
# MAGIC * For more information on Q-Q plots, see http://en.wikipedia.org/wiki/Q%E2%80%93Q_plot.
# MAGIC * **Value** should contain one or two fields.
# MAGIC * **Series Grouping** is always ignored.
# MAGIC * **Keys** can support up to 2 fields.
# MAGIC   * When no key is specified, exactly one quantile plot is output.
# MAGIC   * When 2 fields are specified, then there is a trellis of quantile plots .
# MAGIC * **Aggregation** is not applicable.
# MAGIC * Q-Q Plots are not being calculated on the serverside for now, so only the 1000 rows can be reflected in the plot.

# COMMAND ----------

# MAGIC %sql select * from feature_store.retail_features

# COMMAND ----------

# MAGIC %md
# MAGIC ### A **Box plot** gives you an idea of what the expected range of values are and shows the outliers.
# MAGIC * See http://en.wikipedia.org/wiki/Box_plot for more information on Box Plots.
# MAGIC * **Value** should contain exactly one field.
# MAGIC * **Series Grouping** is always ignored.
# MAGIC * **Keys** can be added.
# MAGIC   * There will be one box and whisker plot for each combination of values for the keys.
# MAGIC * **Aggregation** is not applicable.
# MAGIC * Box plots are not being calculated on the serverside for now, so only the first 1000 rows can be reflected in the plot.
# MAGIC * The Median value of the Box plot is displayed when you hover over the box.

# COMMAND ----------

# MAGIC %sql select * from feature_store.retail_features

# COMMAND ----------

# MAGIC %md ####Images

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/FileStore/shared_uploads/images

# COMMAND ----------

image_df = spark.read.format("image").load("/FileStore/FileStore/shared_uploads/images")
display(image_df)

# COMMAND ----------

# MAGIC %md ###HTML/JS/D3 Visualizations

# COMMAND ----------

displayHTML("<a href ='/files/image.jpg'>Arbitrary Hyperlink</a>")

# COMMAND ----------

# MAGIC %md ###SVG

# COMMAND ----------

displayHTML("""<svg width="400" height="400">
  <ellipse cx="300" cy="300" rx="100" ry="60" style="fill:orange">
    <animate attributeType="CSS" attributeName="opacity" from="1" to="0" dur="5s" repeatCount="indefinite" />
  </ellipse>
</svg>""")

# COMMAND ----------

# MAGIC %md ####D3 Visualization using displayHTML

# COMMAND ----------

lst = (spark.read
               .table("feature_store.retail_features")
               .select("description")
               .where("country_code == 'EN'")
               .distinct().toPandas()["description"].tolist())
type(lst)
word_list = [i for item in lst for i in item.split(" ")]
print(word_list)

# COMMAND ----------

html = """
<!DOCTYPE html>
<meta charset="utf-8">
<script src="https://d3js.org/d3.v4.js"></script>
<script src="https://cdn.jsdelivr.net/gh/holtzy/D3-graph-gallery@master/LIB/d3.layout.cloud.js"></script>
<div id="my_dataviz"></div>
<script>
var words = %s
var margin = {top: 10, right: 10, bottom: 10, left: 10},
    width = 800 - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;
var svg = d3.select("#my_dataviz").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");
var layout = d3.layout.cloud()
  .size([width, height])
  .words(words.map(function(d) { return {text: d}; }))
  .padding(3)
  .fontSize(15)
  .on("end", draw);
layout.start();
function draw(words) {
  svg
    .append("g")
      .attr("transform", "translate(" + layout.size()[0] / 2 + "," + layout.size()[1] / 2 + ")")
      .selectAll("text")
        .data(words)
      .enter().append("text")
        .style("font-size", function(d) { return d.size + "px"; })
        .attr("text-anchor", "middle")
        .attr("transform", function(d) {
          return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
        })
        .text(function(d) { return d.text; });
}
</script>
"""%(word_list)
displayHTML(html)