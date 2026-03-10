# Data Warehouse Naming Conventions

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

## Table of Contents
1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Bronze Rules](#bronze-rules)
   - [Silver Rules](#silver-rules)
   - [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedures](#stored-procedures)

---

## General Principles

- **Naming Conventions:** Use `snake_case`, with lowercase letters and underscores (`_`) to separate words.
- **Language:** Use English for all names.
- **Avoid Reserved Words:** Do not use SQL reserved words as object names.

---

## Table Naming Conventions

### Bronze Rules

All names must start with the source system name, and table names must match their original names without renaming.

**Pattern:** `<sourcesystem>_<entity>`

| Segment | Description |
|---|---|
| `<sourcesystem>` | Name of the source system (e.g., `crm`, `erp`) |
| `<entity>` | Exact table name from the source system |

**Example:** `crm_customer_info` → Customer information from the CRM system.

---

### Silver Rules

All names must start with the source system name, and table names must match their original names without renaming.

**Pattern:** `<sourcesystem>_<entity>`

| Segment | Description |
|---|---|
| `<sourcesystem>` | Name of the source system (e.g., `crm`, `erp`) |
| `<entity>` | Exact table name from the source system |

**Example:** `crm_customer_info` → Customer information from the CRM system.

---

### Gold Rules

All names must use meaningful, business-aligned names for tables, starting with the category prefix.

**Pattern:** `<category>_<entity>`

| Segment | Description |
|---|---|
| `<category>` | Describes the role of the table, such as `dim` (dimension) or `fact` (fact table) |
| `<entity>` | Descriptive name aligned with the business domain (e.g., `customers`, `products`, `sales`) |

**Examples:**
- `dim_customers` → Dimension table for customer data.
- `fact_sales` → Fact table containing sales transactions.

#### Glossary of Category Patterns

| Pattern | Meaning | Example(s) |
|---|---|---|
| `dim_` | Dimension table | `dim_customer`, `dim_product` |
| `fact_` | Fact table | `fact_sales` |
| `report_` | Report table | `report_customers`, `report_sales_monthly` |

---

## Column Naming Conventions

### Surrogate Keys

All primary keys in dimension tables must use the suffix `_key`.

**Pattern:** `<table_name>_key`

| Segment | Description |
|---|---|
| `<table_name>` | Refers to the name of the table or entity the key belongs to |
| `_key` | Suffix indicating this column is a surrogate key |

**Example:** `customer_key` → Surrogate key in the `dim_customers` table.

---

### Technical Columns

All technical columns must start with the prefix `dwh_`, followed by a descriptive name indicating the column's purpose.

**Pattern:** `dwh_<column_name>`

| Segment | Description |
|---|---|
| `dwh_` | Prefix exclusively for system-generated metadata |
| `<column_name>` | Descriptive name indicating the column's purpose |

**Example:** `dwh_load_date` → System-generated column storing the date a record was loaded.

---

## Stored Procedures

All stored procedures used for loading data must follow the naming pattern:

**Pattern:** `load_<layer>`

| Segment | Description |
|---|---|
| `<layer>` | Represents the layer being loaded: `bronze`, `silver`, or `gold` |

**Examples:**
- `load_bronze` → Stored procedure for loading data into the Bronze layer.
- `load_silver` → Stored procedure for loading data into the Silver layer.
- `load_gold` → Stored procedure for loading data into the Gold layer.
