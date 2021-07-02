##This Function is developed for flattening the very nested Json Data/Record into a Dataframe or Equivalent to Spreadsheet type.

def flattenNestedData(nestedDF):
     ##Fetching Complex Datatype Columns from Schema
   fieldNames = dict([(field.name, field.dataType) for field in nestedDF.schema.fields if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

   while len(fieldNames)!=0:
      fieldName=list(fieldNames.keys())[0]

      if type(fieldNames[fieldName]) == StructType:
         extractedFields = [col(fieldName +'.'+ innerColName).alias(innerColName) for innerColName in [ colName.name for colName in fieldNames[fieldName]]]
         nestedDF=nestedDF.select("*", *extractedFields).drop(fieldName)

      elif type(fieldNames[fieldName]) == ArrayType: ##If we enable the ArrayType in Line 2 & 15, we end up having multiple duplicate records. Each array column value will create new record, which is worst.
         nestedDF=nestedDF.withColumn(fieldName,explode_outer(fieldName))

      fieldNames = dict([(field.name, field.dataType) for field in nestedDF.schema.fields if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
   return nestedDF

finalData = flattenNestedData(nestedDF) ## Dataframe you wanted to flatten it up.
display(finalData)
finalData.show()
