# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9365ef1c-eb8a-42d5-a0c6-d0e2f5beaab6",
# META       "default_lakehouse_name": "samalakehousesilver",
# META       "default_lakehouse_workspace_id": "c80302c8-0ac6-4eef-988c-fae517ab7b89",
# META       "known_lakehouses": [
# META         {
# META           "id": "9365ef1c-eb8a-42d5-a0c6-d0e2f5beaab6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # READING DATA FROM SAMALAKE HOUSE

# MARKDOWN ********************

# Reading customer data

# CELL ********************

df1=spark.read.format("csv").\
option("header","true").\
option("inferschema","true").\
load("abfss://Datadepartment@onelake.dfs.\
fabric.microsoft.com/samalakehouse.Lakehouse/Files/Fabric_TutorialAdventureWorks_Customers/AdventureWorks_Customers.csv")
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
