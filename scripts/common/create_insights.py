from base import session

# Create the view with the appropriate metrics
query = """
CREATE OR REPLACE VIEW insights AS
SELECT county,
        COUNT(*) AS sales_count,
        SUM(CAST(price AS int)) AS sales_total,
        MAX(CAST(price AS int)) AS sales_max,
        MIN(CAST(price AS int)) AS sales_min,
        AVG(CAST(price AS int))::numeric(10,2) AS sales_avg
FROM ppr_clean_all
GROUP BY county
ORDER BY sales_count DESC
"""

index = """ 
CREATE INDEX IF NOT EXISTS id_idx
ON ppr_clean_all(id)
"""

statement = """
        SELECT 
                county,
                COUNT(*) AS sales_count,
                SUM(CAST(price AS int)) AS sales_total,
                MAX(CAST(price AS int)) AS sales_max,
                MIN(CAST(price AS int)) AS sales_min,
                AVG(CAST(price AS int))::numeric(10,2) AS sales_avg 
        FROM ppr_clean_all 
        WHERE date_of_sale BETWEEN '2021-01-01' AND '2021-03-31'GROUP BY county ORDER BY sales_count DESC
        """	# Query for the data to be exported
# Execute and commit the query
session.execute(query)
session.commit()