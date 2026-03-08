CREATE DATABASE Customers_transactions;

SET SQL_SAFE_UPDATES = 0;

UPDATE customer_final 
SET Gender = NULL 
WHERE Gender='';


UPDATE customer_final 
SET Age = NULL 
WHERE Age='';

ALTER TABLE customer_final MODIFY AGE INT NULL;

SET SQL_SAFE_UPDATES = 1;

SELECT * FROM customer_final;


CREATE TABLE transactions 
(date_new DATE,
Id_check INT,
ID_client INT,
Count_products DECIMAL(10,3),
Sum_payment Decimal(10,2)
);

SELECT * FROM transactions;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions_final.csv'
INTO TABLE Transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(date_new, Id_check, ID_client, @Count_products, @Sum_payment)
SET
Count_products = CAST(@Count_products AS DECIMAL(10,3)),
Sum_payment = CAST(@Sum_payment AS DECIMAL(10,2));

SELECT COUNT(*) FROM Transactions;

# 1. Помесячная агрегация
WITH monthly AS (
    SELECT 
        t.Id_client,
        DATE_FORMAT(t.date_new, '%Y-%m') AS ym,
        COUNT(*) AS operations_cnt,
        SUM(t.Sum_payment) AS monthly_sum,
        AVG(t.Sum_payment) AS monthly_avg_check
    FROM transactions t
    WHERE t.date_new >= '2015-06-01'
      AND t.date_new <  '2016-06-01'
    GROUP BY t.Id_client, ym
),

# 2. Клиенты без пропусков (12 месяцев)
full_clients AS (
    SELECT Id_client
    FROM monthly
    GROUP BY Id_client
    HAVING COUNT(*) = 12
)

# 3. Итог
SELECT 
    m.Id_client,
    m.ym,
    m.monthly_avg_check,
    m.operations_cnt,
    m.monthly_sum,
    
    AVG(m.monthly_avg_check) OVER (PARTITION BY m.Id_client) AS avg_check_year,
    AVG(m.monthly_sum) OVER (PARTITION BY m.Id_client) AS avg_monthly_sum,
    SUM(m.operations_cnt) OVER (PARTITION BY m.Id_client) AS total_operations_year

FROM monthly m
JOIN full_clients f ON m.Id_client = f.Id_client
ORDER BY m.Id_client, m.ym;

# 2 раздел: информация в разрезе месяцев:

WITH monthly_total AS (
    SELECT 
        DATE_FORMAT(date_new, '%Y-%m') AS ym,
        COUNT(*) AS operations_cnt,
        SUM(Sum_payment) AS monthly_sum,
        AVG(Sum_payment) AS avg_check,
        COUNT(DISTINCT Id_client) AS clients_cnt
    FROM transactions
    WHERE date_new >= '2015-06-01'
      AND date_new <  '2016-06-01'
    GROUP BY ym
),

year_total AS (
    SELECT 
        COUNT(*) AS total_operations,
        SUM(Sum_payment) AS total_sum
    FROM transactions
    WHERE date_new >= '2015-06-01'
      AND date_new <  '2016-06-01'
)

SELECT 
    m.*,
    m.operations_cnt / y.total_operations AS share_operations_year,
    m.monthly_sum / y.total_sum AS share_sum_year
FROM monthly_total m
CROSS JOIN year_total y
ORDER BY ym;

#  % M / F / NA по месяцам + доля затрат

SELECT 
    DATE_FORMAT(t.date_new, '%Y-%m') AS ym,
    c.Gender,
    COUNT(DISTINCT t.Id_client) AS clients_cnt,
    SUM(t.Sum_payment) AS total_sum,
    SUM(t.Sum_payment) / 
        SUM(SUM(t.Sum_payment)) OVER (PARTITION BY DATE_FORMAT(t.date_new, '%Y-%m')) 
        AS share_sum_month
FROM transactions t
JOIN customer_final c ON t.Id_client = c.Id_client
WHERE t.date_new >= '2015-06-01'
  AND t.date_new <  '2016-06-01'
GROUP BY ym, c.Gender
ORDER BY ym;

# 3 часть. Возрастные группы (шаг 10 лет)

SELECT
    age_group,
    CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)) AS quarter,

    COUNT(*) AS operations_cnt,
    SUM(t.Sum_payment) AS total_sum,
    AVG(t.Sum_payment) AS avg_check,

    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)))
    ,2) AS operations_percent,

    ROUND(
        SUM(t.Sum_payment) * 100.0 /
        SUM(SUM(t.Sum_payment)) OVER (PARTITION BY CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)))
    ,2) AS revenue_percent

FROM
(
    SELECT
        t.date_new,
        t.Sum_payment,
        t.Id_client,

        CASE
            WHEN c.Age IS NULL THEN 'NA'
            WHEN c.Age < 20 THEN '<20'
            WHEN c.Age BETWEEN 20 AND 29 THEN '20-29'
            WHEN c.Age BETWEEN 30 AND 39 THEN '30-39'
            WHEN c.Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN c.Age BETWEEN 50 AND 59 THEN '50-59'
            ELSE '60+'
        END AS age_group

    FROM transactions t
    JOIN customer_final c
        ON t.Id_client = c.Id_client

    WHERE t.date_new >= '2015-06-01'
      AND t.date_new <  '2016-06-01'

) t

GROUP BY
    age_group,
    quarter

ORDER BY
    quarter,
    age_group;
