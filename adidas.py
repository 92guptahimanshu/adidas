# Databricks notebook source
#Task1

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sh
# MAGIC wget https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json /temp/ol_cdump.json

# COMMAND ----------

# MAGIC %fs ls file:///databricks/driver/

# COMMAND ----------

##Task2 Part1
df=spark.read.json("file:///databricks/driver/ol_cdump.json")
df.count()

# COMMAND ----------

##Task2 Part 2 (curating data with not to include in results with empty/null "titles" and/or "number of pages" is greater than 20 and #"publishing year" is after 1950
df1=df.select("title","publish_date","publishers","authors","genres","number_of_pages","name").filter(df.title != "null").filter(df.title != "").filter(df.number_of_pages >20).filter(df.publish_date>1950)
df1.count()

# COMMAND ----------

#Task2 Part3.1 Select all "Harry Potter" books
df2=df1.filter(df1.title.contains('Harry Potter'))
display(df2)

# COMMAND ----------

#Task2 Part 3.2 Get the book with the most pages
from pyspark.sql.functions import col, asc,desc
display(df1.select("title","number_of_pages").orderBy(df1.number_of_pages.desc()).take(1))



# COMMAND ----------

#Task2 Part 3.3 Top 5 authors with most written books
from pyspark.sql.functions import explode,col,count
df5=df1.withColumn("authorname",explode("authors.key")).select("authorname","title")
df6=df5.groupBy("authorname").agg(count("title").alias("title_count_perauthor")).orderBy(col("title_count_perauthor").desc()).take(5)
display(df6)

# COMMAND ----------

#Task2 Part 3.4 Top 5 genres with most books
df1.createOrReplaceTempView("tab1")
display(spark.sql("select distinct explode(genres) as genre,count(distinct title) as titlecount from tab1 group by 1 order by 2 desc").take(5))


# COMMAND ----------

#Task2 Part 3.5 Get the avg. number of pages
from pyspark.sql.functions import avg
adf=df1.agg(avg("number_of_pages"))
display(adf)

# COMMAND ----------

#Task2 Part 3.6 Per publish year, get the number of authors that published at least one book
from pyspark.sql.functions import explode,col, count
from pyspark.sql.window import Window
df5=df1.withColumn("authorname",explode("authors.key")).select("authorname","title","publish_date")
df6=df5.groupBy("authorname","publish_date").agg(count("title").alias("title_count_perauthor")).orderBy(col("title_count_perauthor"))
df7=df6.groupBy("publish_date").agg(count("authorname").alias("number_of_authors"))
#df7=df6.filter("authorname =='/authors/OL4740894A'")
display(df7)
