def readFile(spark, fileFormat, fileSchema, input_base_dir, folder_name):
    df= (spark
            .read
            .format(fileFormat)
            .schema(fileSchema)
            .load(f'{input_base_dir}/{folder_name}'))
    return df