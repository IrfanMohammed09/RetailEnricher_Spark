from pyspark.sql.functions import StructType
from pyspark.sql.types import StructField, IntegerType, FloatType, DoubleType


def ordersItemsSchema():
    orderItemSchema=StructType([
        StructField("OrderItem_ID", IntegerType()),
        StructField("OrderItem_OrderID", IntegerType()),
        StructField("OrderItem_ProductID", IntegerType()),
        StructField("OrderItem_ProductQuantity", IntegerType()),
        StructField("OrderItem_TotalValue", DoubleType()),
        StructField("OrderItem_ProductValue", DoubleType())
    ])
    return orderItemSchema