from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Initialize SparkSession
spark = SparkSession.builder.appName("LAB3").getOrCreate()

# A. Load sample data into a DataFrame
# Using the provided dataset and treating the first row as the header
df = spark.read.csv("amazon.csv", header=True, inferSchema=True)

# ============================
# Perform Basic Operations 
# ============================

# Cleaning:
# Filter out missing values and drop duplicate records 
cleaned_df = df.dropna().dropDuplicates() 

# Selecting and Filtering:
# Select books and filter for rows where 'quantity_sold' is greater than 2
filtered_df = cleaned_df.select("order_id", "product_category", "price", "quantity_sold", "customer_region") \
                        .filter((col("product_category") == "Books") & (col("quantity_sold") > 2))

# Show the data frame
filtered_df.show(5)

# ============================
# Perform Advanced Operations 
# ============================
# Write complex SQL queries to extract insights
# Register the cleaned DataFrame as a temporary SQL view
cleaned_df.createOrReplaceTempView("ecommerce_sales")

# Query: Extract the top 5 most frequent product categories
top_categories_sql = spark.sql("""
    SELECT product_category, COUNT(*) AS frequency, CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS revenue
    FROM ecommerce_sales
    GROUP BY product_category
    ORDER BY frequency DESC
    LIMIT 5
""")

print("Top 5 Most Frequent Product Categories:")
top_categories_sql.show()

# Writing DataFrame results to external storage
# Save the results into different formats using the DataFrameWriter interface 
top_categories_sql.write.format("csv").mode("overwrite").save("top_categories_output.csv")