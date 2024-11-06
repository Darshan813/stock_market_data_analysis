# Redshift Serverless connection details
from datetime import datetime
from sqlite3 import OperationalError
from dotenv import load_dotenv
import psycopg2
import os
load_dotenv()

class RedshiftLoader:

    def __init__(self):
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.db_name = os.getenv("DB_NAME")
        self.user = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.conn = None

    def connect(self):
         try:
            print(self.port)
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port= self.port
            )
            print("Connection to Redshift Serverless was successful.")

         except OperationalError as e:
            print("Failed to connect to Redshift Serverless:")

    def create_cursor(self):
        if self.conn is None:
            self.connect()
        return self.conn.cursor()

    def from_s3(self, folder_name, table_name):
        if self.conn is None:
            self.connect()
        cursor = self.conn.cursor()
        s3_bucket = f's3://{self.bucket_name}/{folder_name}'

        create_fact_top_10_stock_data = f"""
        
        DROP TABLE IF EXISTS {table_name};
        
        CREATE TABLE {table_name} (
        "Open" FLOAT,
        "High" FLOAT,
        "Low" FLOAT,
        "Close" FLOAT,
        "Volume" BIGINT,
        "date" DATE,
        "Symbol" VARCHAR(10),
        "last_refreshed" DATE,
        "year" INTEGER,
        "week_no" INTEGER,
        "month" INTEGER,
        "day_no" INTEGER
        );    """

        copy_sql = f"""
            COPY {table_name}
            FROM '{s3_bucket}'
            IAM_ROLE '{os.getenv("IAM_ROLE")}'
            FORMAT AS CSV
            DELIMITER ','
            IGNOREHEADER 1
            TIMEFORMAT 'auto';
            """
        try:
            cursor.execute(create_fact_top_10_stock_data)
            cursor.execute(copy_sql)
            self.conn.commit()
            print(f"Data Successfully Loaded To Redshift Table {table_name}")
        except Exception as e:
            print(f"Error loading data into {e}")
        finally:
            cursor.close()

    def creating_final_stock_table(self):
        cursor = self.create_cursor()
        create_stock_split_data = f"""
        
             DROP TABLE IF EXISTS split_stock_data;
    
             CREATE TABLE split_stock_data AS(
                WITH cte1 as (
                SELECT symbol, split_date,
                LEAD(split_date) OVER (PARTITION BY symbol ORDER BY split_date DESC) as ld_split_date, split_ratio
                FROM stock_split
                ORDER BY split_date DESC
                ),
                
                cte2 as (
                SELECT t2.symbol, t2.ld_split_date,
                t2.split_date - 1 as split_date,
                CAST(exp(sum(ln(t1.split_ratio))) AS DECIMAL) AS split_ratio
                FROM cte1 t1 
                JOIN cte1 t2 
                ON t1.symbol = t2.symbol AND t1.split_date >= t2.split_date
                GROUP BY t2.split_date, t2.ld_split_date, t2.symbol
                ),
                
                cte3 as (
                    SELECT *
                    FROM cte2
                    WHERE ld_split_date IS NULL
                ),
                
                cte4  as (
                SELECT symbol, min(date)
                FROM fact_top_10_stock_data
                GROUP BY symbol
                ), 
                
                cte5 as (
                    SELECT t1.symbol, t2.min as ld_split_date, split_date, t1.split_ratio
                    FROM cte3 t1
                    JOIN cte4 t2
                    ON t1.symbol = t2.symbol
                ),
                
                cte6 as (
                    SELECT * 
                    FROM cte2
                    UNION
                    SELECT * 
                    FROM cte5
                )
                
                SELECT  * FROM cte6
                WHERE ld_split_date IS NOT NULL AND ld_split_date < split_date
                ORDER BY ld_split_date DESC
                
                );
                
                DROP TABLE IF EXISTS final_stock_table;
                
                CREATE TABLE final_stock_table as (
                SELECT f.symbol, close, COALESCE(ROUND(close/split_ratio, 2), close) as split_close_price, date
                FROM fact_top_10_stock_data f
                LEFT JOIN split_stock_data s
                ON s.symbol = f.symbol 
                AND date BETWEEN ld_split_date AND split_date
                ORDER BY date desc
                );
        """

        try:
            cursor.execute(create_stock_split_data)
            self.conn.commit()
            print(f"Final Stock table created in Redshift database")
        except Exception as e:
            print(f"{e}")
        finally:
            cursor.close()

    def executing_sql_procedures(self):
        cursor = self.create_cursor()
        procedure_statements = """
            CREATE OR REPLACE Procedure prev_n_days_pct_change(IN n INT, IN table_name TEXT)
            AS $$
                 DECLARE current_date DATE;
                 prev_date DATE;
                 max_date DATE;
            BEGIN
                max_date := (SELECT max(date) FROM final_stock_table);
                SELECT GETDATE()::DATE INTO current_date;
                prev_date :=    
                CAST(
                    CASE 
                        WHEN EXTRACT(DOW FROM DATEADD(month, n, current_date)) = 6
                        THEN DATEADD(day, 2, DATEADD(month, n, current_date))
                        WHEN EXTRACT(DOW FROM DATEADD(month, n, current_date)) = 0
                        THEN DATEADD(day, 1, DATEADD(month, n, current_date))
                        ELSE DATEADD(month, n, current_date)
                    END 
                        AS DATE);
            
                RAISE NOTICE 'Current Date: %', current_date;
                RAISE NOTICE 'Previous Date: %', prev_date;
            
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_name);
            
                EXECUTE 
                'CREATE TABLE ' || quote_ident(table_name) || ' AS
                SELECT 
                    t1.symbol, 
                    ROUND(((t1.split_close_price - t2.split_close_price) / t2.split_close_price ) * 100, 2) as pct_change
                FROM final_stock_table t1 
                JOIN final_stock_table t2 
                ON t1.symbol = t2.symbol
                WHERE t1.date = ''' || max_date || ''' 
                AND t2.date = ''' || prev_date || '''
                ';
            END;    

            $$ LANGUAGE plpgsql; """

        try:
            cursor.execute(procedure_statements)
            cursor.execute("CALL prev_n_days_pct_change(-1, 'prev_30_days_pct_change')");
            print("prev_30_days_pct_change Table Created.")
            cursor.execute("CALL prev_n_days_pct_change(-3, 'prev_90_days_pct_change')");
            print("prev_90_days_pct_change Table Created.")
            cursor.execute("CALL prev_n_days_pct_change(-6, 'prev_180_days_pct_change')");
            print("prev_180_days_pct_change Table Created.")
            cursor.execute("CALL prev_n_days_pct_change(-12, 'prev_365_days_pct_change')");
            print("prev_365_days_pct_change Table Created.")
            self.conn.commit()
        except Exception as e:
            print(f"Error loading data into Prodecures: {e}")
        finally:
            cursor.close()