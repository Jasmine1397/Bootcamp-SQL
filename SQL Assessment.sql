-- Databricks notebook source
-- MAGIC %md 
-- MAGIC
-- MAGIC **Creating a table**

-- COMMAND ----------



CREATE TABLE Employees__(

EmployeeID INT,

Name VARCHAR(50),

Department VARCHAR(50),

Salary INT,

JoiningDate DATE

);




-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Inserting values**

-- COMMAND ----------

INSERT INTO Employees__ (EmployeeID, Name, Department, Salary, JoiningDate) VALUES

(1, 'Alice', 'Sales', 70000, '2021-03-15'),

(2, 'Bob', 'Sales', 68000, '2022-04-20'),

(3, 'Charlie', 'Marketing', 72000, '2020-07-30'),

(4, 'David', 'Marketing', 75000, '2021-11-25'),

(5, 'Eve', 'Sales', 69000, '2020-02-10'),

(6, 'Frank', 'HR', 66000, '2019-05-15'),

(7, 'Grace', 'HR', 64000, '2021-06-10'),

(8, 'Hannah', 'Finance', 73000, '2022-08-19'),

(9, 'Ian', 'Finance', 71000, '2020-03-05'),

(10, 'Jack', 'Sales', 78000, '2023-01-10'),

(11, 'Kara', 'Marketing', 80000, '2022-05-05'),

(12, 'Liam', 'Finance', 72000, '2021-01-30');

-- COMMAND ----------

select * from Employees__


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 1: Find the total number of employees and the average salary for each department.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

select department, Round(AVG(salary),2) as Average_salary, count(EmployeeID) as Number_of_Employees from employees__
group by Department;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 2: List each employee’s name, department, salary, and their rank based on salary within their department.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

select name, department, Salary, rank() over(partition by department order by salary desc) as Rank_based_on_Salary from employees__;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 3: For each department, find the employee with the highest salary.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

select e.department, e.name, e.salary from employees__ e
where e.salary = (select max(salary) from employees__ where department = e.department);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 4: Calculate the cumulative salary for each employee within their department, ordered by their salary in descending order.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- using sum() as a window function to calculate the running total for each employee
select name, department, salary, sum(salary) over (partition by department order by salary desc) as Cumulative_Salary from employees__;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 5: Find the average salary for each department and list the employees who earn above their department's average salary.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

select e.department, e.salary, e.name from employees__ e
where e.Salary > (select avg(salary) from employees__ where department= e.Department);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 6: For each department, determine the difference between each employee's salary and the highest salary in that department.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT  e.Department, e.Name, e.Salary, (e.Salary - MAX(e.Salary) OVER (PARTITION BY e.Department)) AS SalaryDifference
FROM Employees__ e;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 7: List the number of employees hired each year, ordered by year.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- Count of employees hired on those specific year using year() and count()

select year(JoiningDate) as Hired_Year,count(EmployeeID) as Number_of_Employees_Hired from Employees__
group by year(JoiningDate)
order by Hired_Year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 8: Find the top two highest-paid employees from each department.**

-- COMMAND ----------

-- Retreiving to two highest paid employee using CTE and Rank() for each department

With Top2employees AS (select name, department, salary, rank() over (partition by department order by salary desc) as rank_num 
from employees__)
select name, department, salary
from Top2employees
where rank_num <=2
order by department, salary desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Question 9: Calculate the running average salary for each department, ordered by salary in descending order.**
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- Using CTE AND AVG() AND Partition function to find running average for each department

With Running_Average_Salary AS (select department, salary, avg(salary) over (partition by department order by salary desc) as running_Average from employees__)
select department, salary, round(running_Average, 2) as Running_Average_Salary from Running_Average_Salary
order by department, salary desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 10: Find each employee's tenure in years (as of today) and rank employees by tenure within each department.**

-- COMMAND ----------

SELECT 
    name,
    department,
    joiningDate,
    DATEDIFF(CURRENT_DATE, joiningDate) / 365 AS tenure_years,  -- Calculate tenure in years
    RANK() OVER (PARTITION BY department ORDER BY DATEDIFF(CURRENT_DATE, joiningDate) / 365 DESC) AS tenure_rank
FROM 
    employees__
ORDER BY 
    department, 
    tenure_rank;

