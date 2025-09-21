# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# 
# 
# 
# ## Pipelines in Microsoft Fabric 
# 
# **Definition:**
# A **pipeline** is a sequence of activities that move and process data. Pipelines help automate **data transfer and transformation** while allowing you to manage the flow using **control logic** (branching, loops, etc.). The **graphical canvas** in Microsoft Fabric lets you build pipelines with minimal coding.
# 
# ---
# 
# ### **Core Concepts**
# 
# #### **1. Activities**
# 
# Activities are the building blocks of pipelines—each performs a specific task.
# 
# **Two main types:**
# 
# 1. **Data Transformation Activities**
# 
#    * Move or transform data.
#    * Examples:
# 
#      * **Copy Data:** Extract from a source and load to a destination.
#      * **Data Flow (Gen2):** Apply transformations during transfer.
#      * **Notebook Activities:** Run Spark notebooks.
#      * **Stored Procedure Activities:** Run SQL code.
#      * **Delete Data Activities:** Remove data from a source.
#    * Destinations can include **lakehouse, warehouse, SQL database**, or others.
# 
# 2. **Control Flow Activities**
# 
#    * Manage the **logic of the pipeline**.
#    * Examples: Loops, conditional branching, variables, parameters.
#    * Enables **complex orchestration** of data ingestion and transformation.
# 
# > **Tip:** Check the Microsoft Fabric documentation for a full list of pipeline activities.
# 
# ---
# 
# #### **2. Parameters**
# 
# * Pipelines can accept **parameters** for flexible execution.
# * Example: Use a parameter to **specify a folder name** for storing ingested data.
# * Benefits: **Reusable and flexible pipelines**.
# 
# ---
# 
# #### **3. Pipeline Runs**
# 
# * Each execution of a pipeline is a **pipeline run**.
# * Runs can be:
# 
#   * **On-demand** through the UI
#   * **Scheduled** to run automatically at set times
# * Each run has a **unique ID** to:
# 
#   * Review execution details
#   * Confirm success
#   * Check specific settings used
# 
# ---
# 
# **Key Takeaway:**
# Pipelines in Microsoft Fabric allow **automated, flexible, and orchestrated data workflows** using activities, parameters, and run management—without heavy coding.
# 
# ---
# 
# 


# MARKDOWN ********************

# 
# ## 
# ## **Copy Data Activity in Microsoft Fabric**
# 
# ### **Purpose**
# 
# * Most common pipeline activity.
# * Used to **ingest data** from an external source into a **lakehouse file or table**.
# * Can be used alone or combined with other activities for repeatable ingestion processes.
# 
# ### **Example Workflow**
# 
# 1. **Delete Data Activity** – clears existing data.
# 2. **Copy Data Activity** – loads new data from an external source.
# 3. **Notebook Activity** – transforms and loads data into a table.
# 
# ---
# 
# ### **The Copy Data Tool**
# 
# * A graphical wizard helps configure **source** and **destination**.
# * Supports a wide range of sources: **Lakehouse, Warehouse, SQL Database**, etc.
# 
# ---
# 
# ### **Copy Data Settings**
# 
# * Once added to a pipeline, you can edit its settings in the **pipeline canvas pane**.
# 
# ---
# 
# ### **When to Use Copy Data**
# 
# * When copying data **directly** between a supported source and destination.
# * When you want to **import raw data first** and apply transformations later.
# 
# ---
# 
# ### **When to Use Data Flow Instead**
# 
# * If you need transformations **during ingestion**.
# * If you want to **merge data from multiple sources**.
# * Use **Dataflow Gen2** (via Power Query UI) for defining multiple transformation steps.
# 
# 
# 


# MARKDOWN ********************

# **Use pipeline templates**
# 
# 
# You can define pipelines from any combination of activities you choose, enabling to create custom data ingestion and transformation processes to meet your specific needs. However, there are many common pipeline scenarios for which Microsoft Fabric includes predefined pipeline templates that you can use and customize as required.
# 
# To create a pipeline based on a template, select the Templates tile in a new pipeline as shown here.

# MARKDOWN ********************

# 
# 
# ### **Run and Monitor Pipelines**
# 
# 1. **Validate and Run**
# 
#    * After building a pipeline, use **Validate** to check if the configuration is correct.
#    * You can then run the pipeline:
# 
#      * **Interactively** (run immediately).
#      * Or **Schedule** it to run automatically at set times.
# 
# 2. **View Run History**
# 
#    * The **Run History** shows details of each pipeline run.
#    * You can access it from:
# 
#      * The **pipeline canvas**.
#      * The **pipeline item** in the workspace page.
# 

