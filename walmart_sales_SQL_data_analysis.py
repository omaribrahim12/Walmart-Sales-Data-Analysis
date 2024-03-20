# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Time of day Column

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Time,
# MAGIC (CASE
# MAGIC 		WHEN `time` BETWEEN "00:00:00" AND "12:00:00" THEN "Morning"
# MAGIC         WHEN `time` BETWEEN "12:01:00" AND "16:00:00" THEN "Afternoon"
# MAGIC         ELSE "Evening"
# MAGIC     END) AS time_of_day
# MAGIC
# MAGIC FROM `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC  limit 5
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC ADD COLUMN time_of_day VARCHAR(20);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT time_of_day FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC set time_of_day=
# MAGIC (
# MAGIC   CASE
# MAGIC 		WHEN `time` BETWEEN "00:00:00" AND "12:00:00" THEN "Morning"
# MAGIC         WHEN `time` BETWEEN "12:01:00" AND "16:00:00" THEN "Afternoon"
# MAGIC         ELSE "Evening"
# MAGIC     END
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT time_of_day FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Day name Column

# COMMAND ----------

# MAGIC %sql
# MAGIC select date,
# MAGIC       dayofweek(date)
# MAGIC FROM `walmart`.`default`.`walmart_sales_data_csv` 
# MAGIC limit 5;

# COMMAND ----------

# %sql

# update `walmartsales_dataanalysis`.`default`.`walmart_sales_data_csv`
# SET day_number = DAYOFWEEK(date);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select date,
# MAGIC     DATE_FORMAT(date , 'MM')
# MAGIC
# MAGIC FROM `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC # Business Questions to Ask

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1 How many unique cities does the data have?
# MAGIC
# MAGIC select distinct city 
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In which city is each branch?
# MAGIC SELECT 
# MAGIC 	DISTINCT city,
# MAGIC     branch
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many unique product lines does the data have?
# MAGIC SELECT
# MAGIC 	DISTINCT `product line`
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What is the most selling product line 
# MAGIC SELECT
# MAGIC 	sum(Quantity)  as qty,
# MAGIC   `Product line`
# MAGIC   
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY 2 
# MAGIC ORDER BY 1 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What product line had the largest revenue?
# MAGIC SELECT
# MAGIC 	`product line`,
# MAGIC 	SUM(total) as `total revenue`
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What is the city with the largest revenue?
# MAGIC SELECT
# MAGIC 	branch,
# MAGIC 	city,
# MAGIC 	ROUND(SUM(total), 2) AS total_revenue
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY city, branch 
# MAGIC ORDER BY total_revenue DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What product line had the largest VAT?
# MAGIC SELECT
# MAGIC 	`product line`,
# MAGIC 	ROUND(SUM(`Tax 5%`), 2) as avg_tax
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY `product line`
# MAGIC ORDER BY avg_tax DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fetch each product line and add a column to those product 
# MAGIC -- line showing "Good", "Bad". Good if its greater than average sales
# MAGIC
# MAGIC SELECT 
# MAGIC 	AVG(quantity) AS avg_qnty
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`;
# MAGIC
# MAGIC SELECT
# MAGIC 	`product line`,
# MAGIC 	CASE
# MAGIC 		WHEN AVG(quantity) > 6 THEN "Good"
# MAGIC         ELSE "Bad"
# MAGIC     END AS remark
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY `product line`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- what is the most common payment method 
# MAGIC select distinct Payment, count(Payment) as `Number of transactions` from `walmartsales_dataanalysis`.`default`.`walmart_sales_data_csv` 
# MAGIC group by 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which branch sold more products than average product sold?
# MAGIC select 
# MAGIC 	branch, 
# MAGIC     SUM(quantity) AS qnty
# MAGIC from  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC group by branch
# MAGIC having SUM(quantity) > (select AVG(quantity)  from  `walmart`.`default`.`walmart_sales_data_csv` )
# MAGIC order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- What is the most common product line by gender
# MAGIC SELECT
# MAGIC 	gender,
# MAGIC     `product line`,
# MAGIC     COUNT(gender) AS total_cnt
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY gender, `product line`
# MAGIC
# MAGIC ORDER BY total_cnt DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What is the average rating of each product line
# MAGIC SELECT
# MAGIC 	ROUND(AVG(rating), 2) as avg_rating,
# MAGIC     `product line`
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY `product line`
# MAGIC ORDER BY avg_rating DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Customers Analysis Questions 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many unique customer types does the data have?
# MAGIC SELECT
# MAGIC 	DISTINCT `customer type`
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- How many unique payment methods does the data have?
# MAGIC SELECT
# MAGIC 	DISTINCT payment
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- What is the most common customer type?
# MAGIC SELECT
# MAGIC 	`customer type`,
# MAGIC 	count(*) as count
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY `customer type`
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What is the gender of most of the customers?
# MAGIC SELECT
# MAGIC 	gender,
# MAGIC 	COUNT(*) as gender_cnt
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY gender
# MAGIC ORDER BY gender_cnt DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- What is the gender distribution per branch?
# MAGIC SELECT
# MAGIC 	gender,
# MAGIC 	COUNT(*) as gender_cnt
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC WHERE branch = "C"
# MAGIC GROUP BY gender
# MAGIC ORDER BY gender_cnt DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Which time of the day do customers give most ratings?
# MAGIC SELECT
# MAGIC 	time_of_day,
# MAGIC 	AVG(rating) AS avg_rating
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY time_of_day
# MAGIC ORDER BY avg_rating DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Sales Analysis Questions 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which of the customer types brings the most revenue?
# MAGIC SELECT
# MAGIC 	`Customer type`,
# MAGIC 	SUM(total) AS total_revenue
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY `Customer type`
# MAGIC ORDER BY total_revenue;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which city has the largest tax/VAT percent?
# MAGIC SELECT
# MAGIC 	city,
# MAGIC     ROUND(AVG(`Tax 5%`), 2) AS avg_tax_pct
# MAGIC FROM  `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY city 
# MAGIC ORDER BY avg_tax_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which customer type pays the most in VAT?
# MAGIC SELECT
# MAGIC 	`Customer type`,
# MAGIC 	AVG(`Tax 5%`) AS total_tax
# MAGIC FROM `walmart`.`default`.`walmart_sales_data_csv`
# MAGIC GROUP BY `Customer type`
# MAGIC ORDER BY total_tax;
# MAGIC
