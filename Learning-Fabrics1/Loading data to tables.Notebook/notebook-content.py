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

import os
from pyspark.sql.types import *

for filename in os.listdir("/lakehouse/default/Files/dimension_city"):
    df = spark.read.format('csv') \
        .options(header="true", inferSchema="true") \
        .load("abfss://Datadepartment@onelake.dfs.fabric.microsoft.com/Lakehouse1.Lakehouse/Files/dimension_city/" + filename, on_bad_lines="skip")
    df.write.mode("overwrite").format("delta").saveAsTable("dim_city")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Lakehouse1.dbo.dim_city LIMIT 10;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE Lakehouse1.dbo.dim_city ADD COLUMN newColumn int;
# MAGIC 
# MAGIC UPDATE lakehouse1.dbo.dim_city SET newColumn = 9;
# MAGIC 
# MAGIC SELECT City, newColumn FROM Lakehouse1.dbo.dim_city LIMIT 10;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
