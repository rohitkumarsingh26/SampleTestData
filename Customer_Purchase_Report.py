from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Initiate a Spark Session
sc = SparkContext(master="local", appName="Spark Demo")
spark = SparkSession(sc)

# Read Customer CSV file in dataframe
customer_df = spark.read.option("header","true").csv("C:\\Users\\rohit\\Spark_Learning\\input_data\\starter\\customers.csv")
customer_df.show(5)
customer_df.registerTempTable("Customers")


# Read products CSV file in dataframe
products_df = spark.read.option("header","true").csv("C:\\Users\\rohit\\Spark_Learning\\input_data\\starter\\products.csv")
products_df.show(5)
products_df.registerTempTable("Products")

# Read Transactions CSV file in dataframe
transactions_df= spark.read.json("C:\\Users\\rohit\\Spark_Learning\\input_data\\starter\\transactions")
transactions_df.show(5)

transaction_df1 = transactions_df.withColumn('basket', explode('basket'))
transaction_df2 = transaction_df1.withColumn("price", transaction_df1.basket.price) \
                                 .withColumn("product_id", transaction_df1.basket.product_id)
transaction_df2.show(5)
transaction_df2.registerTempTable("Transactions")

# Calculate the Purchase History
Purchase_History_DF = spark.sql(""" select c.customer_id, c.loyalty_score,
                                  t.product_id, p.product_category,
                                  count(date_of_purchase) as purchase_count
                                  from Customers c left join Transactions t
                                  on c.customer_id=t.customer_id left join
                                  Products p on p.product_id=t.product_id
                                  group by 1,2,3,4""")

Purchase_History_DF.show(10)

# Writing output of the Dataframe to the disk
Purchase_History_DF.write.format("csv").save("C:\\Users\\rohit\\Spark_Learning\\input_data\\Output_Path\\Purchase_History_Temp")


