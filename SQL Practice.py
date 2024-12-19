# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Assessment").getOrCreate()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE com_worker ( worker_id BIGINT , department VARCHAR(25), first_name VARCHAR(25), last_name VARCHAR(25), joining_date DATE, salary BIGINT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO com_worker (worker_id, department, first_name, last_name, joining_date, salary) VALUES  (1, 'HR', 'John', 'Doe', '2020-01-15', 50000), (2, 'IT', 'Jane', 'Smith', '2019-03-10', 60000), (3, 'Finance', 'Emily', 'Jones', '2021-06-20', 75000), (4, 'Sales', 'Michael', 'Brown', '2018-09-05', 60000), (5, 'Marketing', 'Chris', 'Johnson', '2022-04-12', 70000), (6, 'IT', 'David', 'Wilson', '2020-11-01', 80000), (7, 'Finance', 'Sarah', 'Taylor', '2017-05-25', 45000), (8, 'HR', 'James', 'Anderson', '2023-01-09', 65000), (9, 'Sales', 'Anna', 'Thomas', '2020-02-18', 55000), (10, 'Marketing', 'Robert', 'Jackson', '2021-07-14', 60000);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from com_worker;

# COMMAND ----------

# MAGIC %md
# MAGIC You have been asked to find the fifth highest salary without using TOP or LIMIT. Note: Duplicate salaries should not be removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select first_name, last_name, department, dense_rank()over(order by salary desc) as dense_rank from com_worker)
# MAGIC select * from cte
# MAGIC where dense_rank <= 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sf_exchange_rate ( date DATE, exchange_rate FLOAT, source_currency VARCHAR(10), target_currency VARCHAR(10));
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sf_exchange_rate (date, exchange_rate, source_currency, target_currency) VALUES ('2020-01-15', 1.1, 'EUR', 'USD'), ('2020-01-15', 1.3, 'GBP', 'USD'), ('2020-02-05', 1.2, 'EUR', 'USD'), ('2020-02-05', 1.35, 'GBP', 'USD'), ('2020-03-25', 1.15, 'EUR', 'USD'), ('2020-03-25', 1.4, 'GBP', 'USD'), ('2020-04-15', 1.2, 'EUR', 'USD'), ('2020-04-15', 1.45, 'GBP', 'USD'), ('2020-05-10', 1.1, 'EUR', 'USD'), ('2020-05-10', 1.3, 'GBP', 'USD'), ('2020-06-05', 1.05, 'EUR', 'USD'), ('2020-06-05', 1.25, 'GBP', 'USD');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sf_sales_amount ( currency VARCHAR(10), sales_amount BIGINT, sales_date DATE);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sf_sales_amount (currency, sales_amount, sales_date) VALUES ('USD', 1000, '2020-01-15'), ('EUR', 2000, '2020-01-20'), ('GBP', 1500, '2020-02-05'), ('USD', 2500, '2020-02-10'), ('EUR', 1800, '2020-03-25'), ('GBP', 2200, '2020-03-30'), ('USD', 3000, '2020-04-15'), ('EUR', 1700, '2020-04-20'), ('GBP', 2000, '2020-05-10'), ('USD', 3500, '2020-05-25'), ('EUR', 1900, '2020-06-05'), ('GBP', 2100, '2020-06-10');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sf_exchange_rate

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sf_sales_amount

# COMMAND ----------

# MAGIC %md
# MAGIC You work for a multinational company that wants to calculate total sales across all their countries they do business in.
# MAGIC You have 2 tables, one is a record of sales for all countries and currencies the company deals with, and the other holds currency exchange rate information. Calculate the total sales, per quarter, for the first 2 quarters in 2020, and report the sales in USD currency.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     EXTRACT(YEAR FROM s.sales_date) AS year,
# MAGIC     ceil(EXTRACT(MONTH FROM s.sales_date) / 3.0) AS quarter,
# MAGIC     round(SUM(s.sales_amount * e.exchange_rate), 2) AS total_sales_usd
# MAGIC FROM 
# MAGIC     sf_sales_amount s
# MAGIC JOIN 
# MAGIC     sf_exchange_rate e 
# MAGIC ON 
# MAGIC     s.currency = e.source_currency 
# MAGIC     AND s.sales_date = e.date
# MAGIC WHERE 
# MAGIC     s.sales_date BETWEEN '2020-01-01' AND '2020-06-30' 
# MAGIC GROUP BY 
# MAGIC     year, quarter;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE worker (
# MAGIC     department VARCHAR(100),
# MAGIC     first_name VARCHAR(50),
# MAGIC     joining_date DATE,
# MAGIC     last_name VARCHAR(50),
# MAGIC     salary BIGINT,
# MAGIC     worker_id BIGINT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO worker (department, first_name, joining_date, last_name, salary, worker_id) VALUES  ('HR', 'Alice', '2020-01-15', 'Smith', 60000, 1), ('Engineering', 'Bob', '2019-03-10', 'Johnson', 80000, 2), ('Sales', 'Charlie', '2021-07-01', 'Brown', 50000, 3), ('Engineering', 'David', '2018-12-20', 'Wilson', 90000, 4), ('Marketing', 'Emma', '2020-06-30', 'Taylor', 70000, 5);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE titles (
# MAGIC     affected_from DATE,
# MAGIC     worker_ref_id BIGINT,
# MAGIC     worker_title VARCHAR(100)
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO titles (affected_from, worker_ref_id, worker_title) VALUES  ('2020-01-15', 1, 'HR Manager'), ('2019-03-10', 2, 'Software Engineer'), ('2021-07-01', 3, 'Sales Representative'), ('2018-12-20', 4, 'Engineering Manager'), ('2020-06-30', 5, 'Marketing Specialist'), ('2022-01-01', 5, 'Marketing Manager');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from worker;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from titles;

# COMMAND ----------

# MAGIC %md
# MAGIC Find all employees who have or had a job title that includes manager.
# MAGIC Output the first name along with the corresponding title.

# COMMAND ----------

# MAGIC %sql
# MAGIC select w.first_name, t.worker_title from worker w
# MAGIC join titles t
# MAGIC on w.worker_id=t.worker_ref_id
# MAGIC where t.worker_title like '%Manager%';
# MAGIC
# MAGIC
