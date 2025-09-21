# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3a113418-76d4-49cb-831f-9b58a90146ba",
# META       "default_lakehouse_name": "samalakehouse",
# META       "default_lakehouse_workspace_id": "c80302c8-0ac6-4eef-988c-fae517ab7b89",
# META       "known_lakehouses": [
# META         {
# META           "id": "3a113418-76d4-49cb-831f-9b58a90146ba"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Data Engineering**

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Fabric_TutorialAdventureWorks_Calendar/AdventureWorks_Calendar.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Fabric_TutorialAdventureWorks_Calendar/AdventureWorks_Calendar.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
