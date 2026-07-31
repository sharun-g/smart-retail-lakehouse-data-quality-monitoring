# Databricks notebook source
# MAGIC %md
# MAGIC ### customers data

# COMMAND ----------

customers = spark.read.format("delta").load("/Volumes/one/default/lakehouse/bronze/customers")

customers.display()
customers.count()

# COMMAND ----------

customers=customers.dropDuplicates()
customers=customers.dropna()
customers.count()

# COMMAND ----------

customers.select("customer_id").distinct().count()

# COMMAND ----------

customers.write.format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/silver/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### orders data

# COMMAND ----------

orders = spark.read.format("delta").load("/Volumes/one/default/lakehouse/bronze/orders")

# COMMAND ----------

orders.display()
orders.count()

# COMMAND ----------

orders=orders.dropDuplicates()
orders=orders.dropna()

# COMMAND ----------

orders.count()

# COMMAND ----------

from pyspark.sql.functions import col

invalid = orders.filter(
    col("order_delivered_customer_date") < col("order_purchase_timestamp")
)

invalid.count()

# COMMAND ----------

orders.groupBy("order_status").count().display()

# COMMAND ----------

orders.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/silver/orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### products data

# COMMAND ----------

products = spark.read.format("delta").load("/Volumes/one/default/lakehouse/bronze/products")

# COMMAND ----------

products.display()
products.count()

# COMMAND ----------

products=products.dropDuplicates()
products=products.dropna()
products.count()

# COMMAND ----------

products.columns

# COMMAND ----------

from pyspark.sql.functions import col

products.filter(col("product_weight_g") <= 0).count()

products.filter(col("product_length_cm") <= 0).count()

products.filter(col("product_height_cm") <= 0).count()

products.filter(col("product_width_cm") <= 0).count()

# COMMAND ----------

products.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/silver/products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geolocation data

# COMMAND ----------

geolocation = spark.read.format("delta").load('/Volumes/one/default/lakehouse/bronze/geolocation')

# COMMAND ----------

geolocation.count()

# COMMAND ----------


geolocation=geolocation.dropDuplicates()
geolocation=geolocation.dropna()

# COMMAND ----------

geolocation.count()

# COMMAND ----------

geolocation.select("geolocation_zip_code_prefix").distinct().count()

# COMMAND ----------

geolocation.write.format("delta").mode('overwrite').save('/Volumes/one/default/lakehouse/silver/geolocation')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payments data

# COMMAND ----------

payments = spark.read.format("delta").load('/Volumes/one/default/lakehouse/bronze/payments')

# COMMAND ----------

payments.display()
payments.count()

# COMMAND ----------

payments=payments.dropDuplicates()
payments=payments.dropna()

# COMMAND ----------

payments.count()


# COMMAND ----------

payments.groupBy("payment_type").sum("payment_value").display()

# COMMAND ----------

from pyspark.sql.functions import col

payments.filter(col("payment_value") <= 0).display()

# COMMAND ----------

payments.write.format('delta').mode('overwrite').save('/Volumes/one/default/lakehouse/silver/payments')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order_items data

# COMMAND ----------

order_items=spark.read.format("delta").load('/Volumes/one/default/lakehouse/bronze/order_items')
order_items.display()
order_items.count()

# COMMAND ----------

order_items=order_items.dropDuplicates()
order_items=order_items.dropna()
order_items.count()

# COMMAND ----------

from pyspark.sql.functions import col

order_items.filter(col("price") <= 0).count()

# COMMAND ----------

order_items.filter(col("freight_value") < 0).count()

# COMMAND ----------

order_items.write.format("delta").mode('overwrite').save("/Volumes/one/default/lakehouse/silver/order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ### reviews data

# COMMAND ----------

reviews = spark.read.format("csv") \
    .option("header", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_order_reviews_dataset.csv")
reviews.display()
reviews.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### review title and msg can be null as users just gave rating

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
reviews = reviews.withColumn(
    "review_score",
    col("review_score").cast("int")
)

reviews = reviews.withColumn(
    "review_creation_date",
    to_timestamp("review_creation_date")
)

reviews = reviews.withColumn(
    "review_answer_timestamp",
    to_timestamp("review_answer_timestamp")
)

# COMMAND ----------

reviews.printSchema()

# COMMAND ----------

reviews_new = spark.read.format("delta").load(
    "/Volumes/one/default/lakehouse/bronze/reviews"
)

reviews_new.printSchema()

reviews_new.groupBy("review_score").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### issue was in Spark's default CSV parser
# MAGIC ##### fixing

# COMMAND ----------

reviews = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "PERMISSIVE") \
    .load("/Volumes/one/default/lakehouse/raw/olist_order_reviews_dataset.csv")

# COMMAND ----------

reviews.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

reviews.filter(
    col("review_score").contains("-")
).display(20, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

reviews = reviews.withColumn(
    "review_score",
    col("review_score").cast("int")
).withColumn(
    "review_creation_date",
    to_timestamp("review_creation_date")
).withColumn(
    "review_answer_timestamp",
    to_timestamp("review_answer_timestamp")
)

# COMMAND ----------

reviews.groupBy("review_score").count().orderBy("review_score").display()

# COMMAND ----------

from pyspark.sql.functions import col

reviews.filter(col("review_score").isNull()).count()

# COMMAND ----------

reviews = reviews.dropDuplicates()

# COMMAND ----------

reviews = reviews.dropDuplicates(["review_id"])

# COMMAND ----------

reviews.count()

# COMMAND ----------

reviews.write.format('delta').mode('overwrite').save('/Volumes/one/default/lakehouse/silver/reviews')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sellers data

# COMMAND ----------

sellers = spark.read.format('delta').option('header',True).load('/Volumes/one/default/lakehouse/bronze/sellers')
sellers.display()
sellers.count()

# COMMAND ----------

sellers.select('seller_id').distinct().count()

# COMMAND ----------

sellers.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/silver/sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Translation data

# COMMAND ----------

translation = spark.read.format('delta').option('header',True).load('/Volumes/one/default/lakehouse/bronze/translation')
translation.display()
translation.count()

# COMMAND ----------

translation=translation.dropDuplicates()
translation.count()

# COMMAND ----------

translation.write.format('delta').mode('overwrite').save('/Volumes/one/default/lakehouse/silver/translation')