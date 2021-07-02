def flattenNestedData(nestedDF):
   fieldNames = dict([(field.name, field.dataType) for field in nestedDF.schema.fields if type(field.dataType) == StructType]) ##Add for Array --> type(field.dataType) == ArrayType or
   ##Fetching Complex Datatype Columns from Schema
   while len(fieldNames)!=0:
      fieldName=list(fieldNames.keys())[0]
      print ("Processing :"+fieldName+" Type : "+str(type(fieldNames[fieldName])))

      if (type(fieldNames[fieldName]) == StructType):
         extractedFields = [col(fieldName +'.'+ innerColName).alias(innerColName) for innerColName in [ colName.name for colName in fieldNames[fieldName]]]
         nestedDF=nestedDF.select("*", *extractedFields).drop(fieldName)

      elif (type(fieldNames[fieldName]) == ArrayType):
         nestedDF=nestedDF.withColumn(fieldName,explode_outer(fieldName))

      fieldNames = dict([(field.name, field.dataType) for field in nestedDF.schema.fields if type(field.dataType) == StructType]) ##Add for Array --> type(field.dataType) == ArrayType or
   return nestedDF

finalDF = flattenNestedData(nestedDF)
display(finalDF)
finalDF.show()
