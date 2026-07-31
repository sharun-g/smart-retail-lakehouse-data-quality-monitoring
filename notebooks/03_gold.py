# Databricks notebook source
orders=spark.read.format('delta').option('header','true').option('inferSchema','true').load('/Volumes/one/default/lakehouse/silver/orders/')

customers=spark.read.format('delta').option('header','true').option('inferSchema','true').load('/Volumes/one/default/lakehouse/silver/customers/')

payments=spark.read.format('delta').option('header','true').option('inferSchema','true').load('/Volumes/one/default/lakehouse/silver/payments/')

order_items=spark.read.format('delta').option('header','true').option('inferSchema','true').load('/Volumes/one/default/lakehouse/silver/order_items/')

products=spark.read.format('delta').option('header','true').option('inferSchema','true').load('/Volumes/one/default/lakehouse/silver/products/')

sellers=spark.read.format('delta').option('header','true').option('inferSchema','true').load('/Volumes/one/default/lakehouse/silver/sellers/')

translation = spark.read.format("delta").option('header','true').option('inferSchema','true').load("/Volumes/one/default/lakehouse/silver/translation")

reviews=spark.read.format('delta').option('header','true').option('inferSchema','true').load('/Volumes/one/default/lakehouse/silver/reviews')

# COMMAND ----------

from pyspark.sql.functions import sum, first

payments_gold = payments.groupBy("order_id").agg(
    sum("payment_value").alias("payment_value"),
    first("payment_type").alias("payment_type"),
    first("payment_installments").alias("payment_installments")
)

# COMMAND ----------

from pyspark.sql.functions import avg

reviews_gold = reviews.groupBy("order_id").agg(
    avg("review_score").alias("review_score")
)

# COMMAND ----------

fact_sales = orders.join(customers, on="customer_id",how="left")

# COMMAND ----------

fact_sales = fact_sales.join(order_items, on="order_id", how="left")

# COMMAND ----------

fact_sales = fact_sales.join(products, on="product_id", how="left")

# COMMAND ----------

fact_sales = fact_sales.join(translation, on="product_category_name", how="left")

# COMMAND ----------

fact_sales = fact_sales.join(sellers,on="seller_id",how="left")

# COMMAND ----------

fact_sales = fact_sales.join(payments_gold,on="order_id",how="left")

# COMMAND ----------

fact_sales = fact_sales.join(reviews_gold,on="order_id",how="left")

# COMMAND ----------

fact_sales.count()

# COMMAND ----------

fact_sales.display()

# COMMAND ----------

from pyspark.sql.functions import *

fact_sales.select([count(when(col(c).isNull(), c)).alias(c) 
                   for c in ["order_id","customer_id","product_id","seller_id","price","payment_value"]]).display()

# COMMAND ----------

payments_gold.groupBy("order_id") \
             .count() \
             .filter("count > 1") \
             .count()

# COMMAND ----------

fact_sales.filter(col("payment_value").isNull()) \
          .select("order_id", "customer_id", "order_status") \
          .display()

# COMMAND ----------

fact_sales.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/gold/fact_sales")

# COMMAND ----------

from pyspark.sql.functions import *

product_performance = fact_sales.groupBy(
    "product_id",
    "product_category_name_english"
).agg(
    count("*").alias("units_sold"),
    countDistinct("order_id").alias("total_orders"),
    sum("price").alias("revenue"),
    avg("price").alias("avg_price"),
    avg("review_score").alias("avg_review_score")
)

product_performance.display()

# COMMAND ----------

product_performance.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/gold/product_performance")

# COMMAND ----------

seller_performance = fact_sales.groupBy(
    "seller_id",
    "seller_city",
    "seller_state"
).agg(
    countDistinct("order_id").alias("orders"),
    count("product_id").alias("products_sold"),
    sum("price").alias("revenue"),
    avg("review_score").alias("avg_review_score")
)

seller_performance.display()

# COMMAND ----------

seller_performance.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/gold/seller_performance")

# COMMAND ----------

order_summary = fact_sales.groupBy(
    "order_id",
    "customer_id",
    "customer_city",
    "customer_state"
).agg(
    first("payment_value").alias("payment_value"),
    first("order_purchase_timestamp").alias("purchase_date")
)

# COMMAND ----------

customer_summary = order_summary.groupBy(
    "customer_id",
    "customer_city",
    "customer_state"
).agg(
    count("order_id").alias("total_orders"),
    sum("payment_value").alias("total_spent"),
    avg("payment_value").alias("avg_order_value"),
    max("purchase_date").alias("last_purchase")
)

customer_summary.display()

# COMMAND ----------

customer_summary.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/gold/customer_summary")

# COMMAND ----------

daily_sales_summary = order_summary.groupBy(
    to_date("purchase_date").alias("order_date")
).agg(
    count("order_id").alias("total_orders"),
    sum("payment_value").alias("daily_revenue"),
    avg("payment_value").alias("avg_order_value")
)

daily_sales_summary.display()

# COMMAND ----------

daily_sales_summary.write.format("delta").mode("overwrite").save("/Volumes/one/default/lakehouse/gold/daily_sales_summary")