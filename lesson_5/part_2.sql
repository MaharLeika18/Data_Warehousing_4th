WITH MonthlyStoreRevenue AS (
    SELECT  
        store_id, 
        ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
        strftime('%m', DATETIME(invoice_date, 'unixepoch')) AS month,     -- assumes datetime values are stored as int
        strftime('%Y', DATETIME(invoice_date, 'unixepoch')) AS year,
    FROM fact_sales
    GROUP BY store_id, year, month 
),
WithMoMRevVariance AS (
    SELECT
        store_id,
        year,
        month,
        total_revenue,
        total_revenue - LAG(total_revenue) OVER (
            PARTITION BY store_id ORDER BY year, month
        ) AS mom_revenue_variance
    FROM MonthlyStoreRevenue
)
SELECT * FROM WithMoMRevVariance
ORDER BY store_id, year, month;

-- Using a self-join can compute the CTE once but then store it twice, as the previous and current total_revenue, when calculating the MoM. 
-- Additionally, the self-join would need a calculated join condition for the previous month and would have to be evaluated per row pair candidate
-- instead of once per row.

-- In terms of structure, the window function LAG reads naturally as looking once step back for every store whereas the self-join has to be thought
-- of as two separate set being combined by a matching condition which is a lot more indirect and obscure. The window function also has the benefit 
-- if being easily extendable for things like rolling averages being computable with single line changes. With the self-join, each extension would
-- need either a new join or CTE which further increases the memory consumption. 