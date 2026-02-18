# Data Catalog for Gold Layer

## Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact tables** organized in a star schema for dimensional analysis. Data is transformed from the silver layer with business logic applied, including data reconciliation between CRM and ERP sources.

---

### 1. **gold.dim_customers**
- **Purpose:** Stores customer details enriched with demographic and geographic data from both CRM and ERP systems.
- **Source Tables:** silver.crm_cust_info (primary), silver.erp_cust_az12, silver.erp_loc_a101
- **Data Quality:** Gender values are reconciled between CRM and ERP sources, with 'n/a' values replaced with 'Unknown'. CRM data is prioritized when available.
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| customer_key     | INT           | Surrogate key uniquely identifying each customer record in the dimension table. Generated using ROW_NUMBER(). |
| customer_id      | INT           | Unique numerical identifier assigned to each customer by the CRM system.                       |
| customer_number  | NVARCHAR(50)  | Alphanumeric identifier representing the customer, used for tracking and referencing.         |
| first_name       | NVARCHAR(100) | The customer's first name, as recorded in the CRM system.                                     |
| last_name        | NVARCHAR(100) | The customer's last name or family name, as recorded in the CRM system.                       |
| gender           | NVARCHAR(50)  | The gender of the customer ('Male', 'Female', or 'Unknown'). Reconciled from CRM and ERP sources with null/n/a handling. |
| birth_date       | DATE          | The date of birth of the customer, sourced from the ERP system, formatted as YYYY-MM-DD.     |
| country          | NVARCHAR(50)  | The country of residence for the customer, sourced from the ERP location data.               |
| marital_status   | NVARCHAR(50)  | The marital status of the customer ('Single', 'Married', or 'Unknown'), sourced from CRM.    |
| create_date      | DATE          | The date when the customer record was created in the CRM system, formatted as YYYY-MM-DD.    |

---

### 2. **gold.dim_products**
- **Purpose:** Provides comprehensive product information with categorization and maintenance flags. Includes only active products.
- **Source Tables:** silver.crm_prd_info (primary), silver.erp_px_cat_g1v2
- **Data Quality:** Includes only products where prd_end_dt IS NULL (active products). Products are ordered by start date and product key.
- **Columns:**

| Column Name         | Data Type     | Description                                                                                   |
|---------------------|---------------|-----------------------------------------------------------------------------------------------|
| product_key         | INT           | Surrogate key uniquely identifying each product record in the product dimension table. Generated using ROW_NUMBER(). |
| product_id          | INT           | A unique identifier assigned to the product by the CRM system for internal tracking.          |
| product_number      | NVARCHAR(50)  | A structured alphanumeric code representing the product, used for categorization and inventory management. |
| product_name        | NVARCHAR(100) | Descriptive name of the product, including key details such as type, color, and size.         |
| category_id         | NVARCHAR(50)  | A unique identifier for the product's category, linking to categorization in the ERP system.  |
| category            | NVARCHAR(50)  | The broader classification of the product (e.g., Bikes, Components), sourced from ERP.       |
| subcategory         | NVARCHAR(50)  | A more detailed classification of the product within the category, such as product type (e.g., Road Bikes). |
| maintenance_flag    | NVARCHAR(50)  | Indicates whether the product requires maintenance ('Yes', 'No', or 'Unknown'), sourced from ERP. |
| cost                | INT           | The cost or base price of the product in whole monetary units, sourced from the CRM system.   |
| product_line        | NVARCHAR(50)  | The specific product line or series to which the product belongs (e.g., Road, Mountain).      |
| start_date          | DATE          | The date when the product became available for sale or use, formatted as YYYY-MM-DD.         |

---

### 3. **gold.fact_sales**
- **Purpose:** Stores transactional sales data for analytical purposes, linked to customer and product dimensions via surrogate keys.
- **Source Tables:** silver.crm_sales_details (primary), gold.dim_products, gold.dim_customers
- **Data Quality:** Linked to dimension tables via product_key and customer_key. Records with non-existent dimension keys will have NULL values.
- **Columns:**

| Column Name     | Data Type     | Description                                                                                   |
|-----------------|---------------|-----------------------------------------------------------------------------------------------|
| order_number    | NVARCHAR(50)  | A unique alphanumeric identifier for each sales order (e.g., 'SO54496'), sourced from CRM.   |
| product_key     | INT           | Surrogate key linking the order to the product dimension table (gold.dim_products).           |
| customer_key    | INT           | Surrogate key linking the order to the customer dimension table (gold.dim_customers).         |
| order_date      | DATE          | The date when the order was placed, formatted as YYYY-MM-DD.                                  |
| ship_date       | DATE          | The date when the order was shipped to the customer, formatted as YYYY-MM-DD.                |
| due_date        | DATE          | The date when the order payment was due, formatted as YYYY-MM-DD.                             |
| sales           | INT           | The total monetary value of the sale for the line item in whole currency units.               |
| quantity        | INT           | The number of units of the product ordered for the line item.                                |
| price           | INT           | The price per unit of the product for the line item in whole currency units.                  |

---

## Data Integration Notes

- **Medallion Architecture:** Data flows from Bronze (raw) → Silver (cleansed) → Gold (business-ready).
- **Key Transformations in Gold Layer:**
  - Customer surrogate keys generated using ROW_NUMBER() for dimensional analysis
  - Product surrogate keys generated using ROW_NUMBER() ordering by start date and product key
  - Gender reconciliation prioritizes CRM data with ERP fallback
  - Active products only (where end_date IS NULL)
  - NULL/n/a value handling converts 'n/a' to 'Unknown' for consistency

- **Relationships:**
  - fact_sales links to dim_customers via customer_key
  - fact_sales links to dim_products via product_key

