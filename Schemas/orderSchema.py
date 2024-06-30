from pyspark.sql.types import StructField, IntegerType, DateType, StringType, StructType
def ordersSchema():
    orderSchema=StructType([
        StructField("OrderID", IntegerType()),
        StructField("OrderDate", DateType()),
        StructField("OrderCustomerID", IntegerType()),
        StructField("OrderStatus", StringType())
    ])
    return orderSchema