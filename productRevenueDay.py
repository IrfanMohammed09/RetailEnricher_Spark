from pyspark.sql.functions import sum, desc


def calculateTotalProductRevenue_day(order_df, order_items_df):
    filter_df = order_df.filter('OrderStatus in ("COMPLETE", "CLOSED")')
    joined_df=filter_df.join(order_items_df, order_df.OrderID == order_items_df.OrderItem_OrderID)
    agg_df=joined_df.groupBy("OrderItem_ProductID", "OrderDate").agg(sum("OrderItem_TotalValue").alias("TotalProductValue"))
    final_df=agg_df.orderBy("OrderDate",desc("TotalProductValue"))
    return final_df