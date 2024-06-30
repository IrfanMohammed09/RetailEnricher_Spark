import sys
from constants.DevConstants import INPUT_BASE_DIR
from Schemas.OrderItemsSchema import ordersItemsSchema
from constants.sourceConstants import ORDERS_FOLDER_NAME, ORDER_ITEMS_FOLDER_NAME, FILE_FORMAT
from Schemas.orderSchema import ordersSchema
from productRevenueDay import calculateTotalProductRevenue_day
from reader import readFile
from sparkSessionBuilder import buildSparkSession
import configparser as cp


def main():
    environment=sys.argv[1]
    source=sys.argv[2]
    sink=sys.argv[3]
    props=cp.RawConfigParser()
    props.read("ConfigurationFile/application.properties")
    input_base_dir=props.get(environment,INPUT_BASE_DIR)
    ordersFolder=props.get(source, ORDERS_FOLDER_NAME)
    orderItemsFolder=props.get(source, ORDER_ITEMS_FOLDER_NAME)
    fileFormat=props.get(source, FILE_FORMAT)
    spark=buildSparkSession("Retail_App")
    orderSchema=ordersSchema()
    orderItemSchema=ordersItemsSchema()
    order_df=readFile(spark, fileFormat, orderSchema, input_base_dir, ordersFolder)
    order_items_df=readFile(spark, fileFormat, orderItemSchema,input_base_dir, orderItemsFolder)
    final_df=calculateTotalProductRevenue_day(order_df, order_items_df)
    final_df.show()


if __name__ =="__main__":
    main()
