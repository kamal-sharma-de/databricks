# Create SparkSession
spark = SparkSession.builder.appName("EcommerceSalesAnalysis").getOrCreate()

# Set log level (optional)
spark.sparkContext.setLogLevel("WARN")

def analyze_sales_data(source_file_path, target_table_name):
  """
  Analyzes e-commerce sales data, calculates metrics, and writes to Delta Lake

  Args:
      source_file_path (str): Path to the source sales data file (DBFS)
      target_table_name (str): Name of the target Delta Lake table
  """
  try:
    # Define data path
    data_path = f"dbfs:/{source_file_path}.csv"

    # Read sales data as DataFrame
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Filter for completed orders
    filtered_df = df.filter(df.order_status == "COMPLETED")

    # Calculate total sales and number of orders
    total_sales = filtered_df.groupBy().agg(sum("order_total").alias("total_sales"))
    num_orders = filtered_df.groupBy().count()

    # Calculate average order value
    avg_order_value = total_sales.select(total_sales.total_sales / num_orders.alias("total_orders")).withColumnRenamed("((total_sales / total_orders))", "average_order_value")

    # Combine results into a single DataFrame
    sales_analysis = total_sales.join(num_orders, broadcast=True).join(avg_order_value, broadcast=True)

    # Define target table path
    table_path = f"deltaTable/{target_table_name}"

    # Write analysis results to Delta Lake
    sales_analysis.write.format("delta").mode("overwrite").save(table_path)

    print(f"E-commerce sales analysis results written to: {table_path}")
  except:
    print("Error during data processing or writing. Check data and table details.")

# Replace placeholders with your data and table details
analyze_sales_data("<source_file_path>", "sales_analysis")
