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
# ## Introduction to Fabric Lakehouse & Medallion Architecture
# 
# * **Why it matters**: Businesses need to manage and analyze large, diverse data sources effectively.
# * **Solution**: The **Fabric lakehouse** combines the strengths of data lakes and data warehouses.
# * **Industry Standard**: The **Medallion Architecture** (Bronze, Silver, Gold layers) is widely used to organize and refine data.
# 
# ### Key Points:
# 
# * **Bronze Layer** – Raw data ingestion.
# * **Silver Layer** – Data cleaning, refinement, and transformation.
# * **Gold Layer** – Curated, business-ready data for analytics and reporting.
# 
# ### What you’ll learn in this module:
# 
# 1. Build and organize a **medallion architecture** in Fabric.
# 2. Query and create reports from your Fabric lakehouse.
# 3. Apply **best practices** for security and governance.
# 
# .
# 


# MARKDOWN ********************

# 
# 
# ## Medallion Architecture in Fabric (Delta Lake)
# 
# **Definition:**
# A **data design pattern** for organizing data in a **lakehouse**. It improves data quality step by step across different layers: **Bronze → Silver → Gold**.
# 
# * Ensures **ACID transactions** (Atomicity, Consistency, Isolation, Durability).
# * Makes data **reliable, consistent, and analysis-ready**.
# * Complements (not replaces) other data organization methods.
# 
# ---
# 
# ### Layers of the Medallion Architecture
# 
# 1. **Bronze Layer (Raw)**
# 
#    * Landing zone for all incoming data (structured, semi-structured, unstructured).
#    * Data is stored **as-is** in its original format.
#    * No cleaning or transformation done here.
# 
# 2. **Silver Layer (Validated)**
# 
#    * Data is **cleaned, validated, and standardized**.
#    * Activities: remove nulls, deduplicate, merge data.
#    * Acts as a **central repository** accessible by multiple teams.
#    * Prepares data for refinement in the Gold layer.
# 
# 3. **Gold Layer (Enriched)**
# 
#    * Data is **refined for business needs** (analytics, ML, reporting).
#    * Activities: aggregation (e.g., daily/hourly), enrichment with external sources.
#    * Data is **ready for use by downstream teams** (BI, Data Science, MLOps).
# 
# ---
# 
# ### Customization
# 
# * Layers are **flexible** – you can add extra ones, e.g.:
# 
#   * *Raw Layer* (before Bronze).
#   * *Platinum Layer* (specialized business-ready datasets).
# * Adapt based on organizational needs.
# 
# ---
# 
# ### Moving Data Across Layers in Fabric
# 
# **Purpose:** To refine, organize, and prepare data for use.
# 
# **Considerations:**
# 
# * Data size.
# * Complexity of transformations.
# * Frequency of movement.
# * Tools familiarity.
# 
# **Tools:**
# 
# * **Data Transformation**
# 
#   * *Dataflows (Gen2):* simple models & small transformations.
#   * *Notebooks:* complex transformations & large models (can save as Delta tables).
# 
# * **Data Orchestration**
# 
#   * *Pipelines:* manage and automate movement between layers (scheduled or event-based).
# 
# ---
# 
# ✅ **Key takeaway:**
# The **Medallion Architecture** (Bronze → Silver → Gold) provides a structured, step-by-step way of cleaning, validating, and enriching data in a lakehouse, making it **reliable, scalable, and analytics-ready**.
# 
# 


# MARKDOWN ********************

# 
# 
# ### **1. Foundation Setup**
# 
# * **Create a Fabric Lakehouse** → This is your storage foundation.
# * You can use **one lakehouse** for multiple medallion setups or separate lakehouses per use case/domain.
# 
# ---
# 
# ### **2. Architecture Design**
# 
# * Define **layers** and **data flow** between them:
# 
#   * **Bronze (Raw Layer):** Ingest raw data.
#   * **Silver (Curated Layer):** Cleanse, validate, ensure quality & consistency.
#   * **Gold (Presentation Layer):** Transform, model, and optimize for reporting.
# 
# * **Gold Layer** → Use **star schema** and design for reporting/analytics.
# 
# | **Layer** | **What Happens**            | **Tools Used**                         |
# | --------- | --------------------------- | -------------------------------------- |
# | Bronze    | Ingest raw data             | Pipelines, dataflows, notebooks        |
# | Silver    | Cleanse & validate          | Dataflows, notebooks                   |
# | Gold      | Transform, model, reporting | SQL analytics endpoint, semantic model |
# 
# ---
# 
# ### **3. Step-by-Step Implementation**
# 
# 1. **Ingest Data → Bronze**
# 
#    * Use **pipelines, dataflows, or notebooks** to load raw data.
# 
# 2. **Transform Data → Silver**
# 
#    * Cleanse and standardize data for quality/consistency.
#    * Use **dataflows or notebooks** (not focused on modeling yet).
# 
# 3. **Model Data → Gold**
# 
#    * Create **dimensional/star schema models** for reporting.
#    * Define **relationships, measures, KPIs, and hierarchies**.
#    * Multiple gold layers can be created for **different teams (finance, sales, ML, etc.)**.
#    * Optionally, use a **Data Warehouse** for Gold.
# 
# ---
# 
# ### **4. Tools & Consumption**
# 
# * **Transformations** → Dataflows or notebooks.
# * **Gold storage** → Delta tables in Lakehouse.
# * **Access** →
# 
#   * Connect via **SQL analytics endpoint**.
#   * Use **Power BI** for reports & dashboards.
# * **Permissions** → Control downstream consumption with **workspace/item permissions**.
# 
# ---
# 
# 👉 In short: **Bronze = Raw ingestion → Silver = Clean, consistent data → Gold = Modeled, ready for analytics & reporting.**
# 


# MARKDOWN ********************

# 
# 
# ## Query and Report on Data in Fabric Lakehouse
# 
# ### 1. Querying Data
# 
# * Use **SQL** to explore/query data, especially in the **Gold layer**.
# * You can:
# 
#   * Analyze data in **Delta tables** (any layer).
#   * Create **functions & views**.
#   * Apply **SQL security**.
# * **SQL Analytics Endpoint** lets you:
# 
#   * Write queries.
#   * Manage semantic models.
#   * Use a **visual query experience**.
#   * ⚠️ Note: It is **read-only** (for changes, use dataflows, notebooks, or pipelines).
# 
# ---
# 
# ### 2. Reporting Data
# 
# * Use **Power BI Semantic Models**:
# 
#   * Provides a **business-friendly view** of data.
#   * Organized as a **star schema** (facts + dimensions).
#   * A **default model** is auto-created with a lakehouse.
#   * You can also create **custom models**.
# * **Direct Lake Mode**: Connects semantic models directly to **Delta tables** for faster insights.
# 
# ---
# 
# ### 3. Tailoring Medallion Layers
# 
# * **Customize layers** to meet needs of different teams (finance, sales, data science, etc.).
# * Multiple **Gold layers** can be created for different audiences.
# * Helps ensure:
# 
#   * Better **performance**.
#   * Greater **ease of use**.
#   * Higher **relevance** for each group.
# * Medallion architecture can also prepare **special formats** required by apps or external systems.
# 
# ---
# 
# 👉 In short:
# 
# * **SQL Analytics Endpoint** = query & explore (read-only).
# * **Power BI Semantic Models** = reporting & business-friendly view.
# * **Tailored Gold Layers** = flexibility for different users.
# 


# MARKDOWN ********************

# 
# 
# ## 🔒 Securing Your Lakehouse
# 
# * **Control access with permissions**
# 
#   * *Workspace permissions* → access to everything in a workspace.
#   * *Item-level permissions* → access to specific items only. Useful for external collaborators.
# 
# * **Use separate workspaces for layers** → improves security and cost-efficiency.
# 
#   * **Gold Layer** → Read-only access (only trusted, validated data).
#   * **Silver Layer** → Decide if users can build on it (balance flexibility & security).
#   * **Bronze Layer** → Read-only access (raw data should stay untouched).
# 
# * **Always align with your organization’s security policies** before sharing Fabric content.
# 
# ---
# 
# ## ⚙️ Continuous Integration & Continuous Delivery (CI/CD)
# 
# * **Purpose**: Automates data pipeline deployment, improves reliability, reduces errors.
# 
# * **Key Considerations**:
# 
#   * Data quality checks
#   * Version control
#   * Automated deployments
#   * Monitoring & security
#   * Scalability & disaster recovery
#   * Collaboration & compliance
#   * Continuous improvement
# 
# * **Git Integration in Fabric**:
# 
#   * Back up & version work
#   * Revert changes when needed
#   * Collaborate via branches
#   * Use familiar Git workflows inside Fabric
# 
# * **Gold Layer CI/CD**:
# 
#   * Most critical layer for CI/CD.
#   * Ensures data is **validated, reliable, and always up-to-date**.
#   * Automates integration of new data & transformations.
#   * Provides consistent, accurate insights for downstream users.
# 
# ---
# 
# ✅ **Bottom line**:
# 
# * Secure access by using layered permissions.
# * Automate CI/CD (especially for the Gold layer) to ensure reliable, high-quality data and efficient collaboration.
# 
# 

