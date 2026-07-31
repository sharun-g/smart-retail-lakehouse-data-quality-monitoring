# Databricks notebook source
# MAGIC %md
# MAGIC ### data reading and inserting into Bronze folder

# COMMAND ----------

customers = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_customers_dataset.csv")

customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/customers")

# COMMAND ----------

orders = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_orders_dataset.csv")

orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/orders")

# COMMAND ----------

products = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_products_dataset.csv")

products.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/products")

# COMMAND ----------

sellers = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_sellers_dataset.csv")

sellers.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/sellers")

# COMMAND ----------

geolocation = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_geolocation_dataset.csv")

geolocation.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/geolocation")

# COMMAND ----------

order_items = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_order_items_dataset.csv")

order_items.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/order_items")

# COMMAND ----------

payments = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_order_payments_dataset.csv")

payments.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/payments")

# COMMAND ----------

reviews = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/olist_order_reviews_dataset.csv")

reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/reviews")

# COMMAND ----------

translation = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/one/default/lakehouse/raw/product_category_name_translation.csv")

translation.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/one/default/lakehouse/bronze/translation")