# Data-warehouse-snowflake-dbt

### 1. Introduction
AdventureWorks is a fictional manufacturing company in need of an advanced data solution to efficiently manage and analyze its business operations. The company relied on a transactional database (AdventureWorks2019) to store information about products, customers, orders, and sales. However, the existing system lacked the ability to perform historical analysis, generate meaningful reports, and support business intelligence (BI) requirements. The objective of this project was to design and implement a robust data warehouse (AdventureWorks_DWH) and develop reports to enable data-driven decision-making.

### 2. Overview
**Data Warehouse Modeling and Architecture**: Designed and implemented a modern data warehouse on Snowflake using the Medallion architecture (Bronze, Silver, Gold layers) to structure and organize data efficiently.

**Data Ingestion and Transformation**: Utilized Apache Airflow to orchestrate data loading workflows from the transactional source system into Snowflake. Applied dbt (data build tool) to perform transformations across Medallion layers, ensuring data quality, reusability, and scalability.

**Dashboard**: Built interactive dashboards directly within Snowflake's native dashboarding capabilities, 

### 3. Source Database 

**Product-related tables**:

![product](https://github.com/user-attachments/assets/85c15113-2ddf-4653-a17e-5ae1bd0506d9)

**Customer-related tables**: 

![Customer](https://github.com/user-attachments/assets/655abb0d-30eb-4e88-a8be-b046e68d16f2)


**Order-related tables**: 

![Sales](https://github.com/user-attachments/assets/18bedd4a-31cf-4c2f-9f5b-f764afa3b488)


**Shipping and delivery**: 

![Shipmethod](https://github.com/user-attachments/assets/1380a828-8976-4a74-9e68-5c5400d7d2da)


**Employee and Sales**:

![Salesperson](https://github.com/user-attachments/assets/c354b291-d8af-48d0-982a-31198b3e6ca0)



### 4. Data Architecture

![Ảnh chụp màn hình 2025-06-10 191902](https://github.com/user-attachments/assets/11b2b416-67ef-4a14-9c3a-6096d5aaa82c)

### 5. Data Warehouse Modeling 

![image](https://github.com/user-attachments/assets/b9fb218c-db4d-4a26-b5aa-ba6b326b3763)

The data warehouse (AdventureWorks_DWH) was designed using a star schema approach. The schema consists of:

**Fact Tables**: Central tables that store quantitative data (e.g., sales facts).

**Dimension Tables**: Surrounding tables that store descriptive attributes (e.g., customer, product, date, and shipping dimensions).
### 6. Dashboard
