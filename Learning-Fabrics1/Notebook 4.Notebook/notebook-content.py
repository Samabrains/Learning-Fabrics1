# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "49d11f17-17b2-4f96-941b-202ab7f1d585",
# META       "default_lakehouse_name": "Lakehouse1",
# META       "default_lakehouse_workspace_id": "c80302c8-0ac6-4eef-988c-fae517ab7b89",
# META       "known_lakehouses": [
# META         {
# META           "id": "49d11f17-17b2-4f96-941b-202ab7f1d585"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Lakehouse1.dbo.nycsample LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
