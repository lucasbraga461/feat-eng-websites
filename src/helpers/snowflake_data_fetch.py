import os
import pandas as pd
import snowflake.connector

class SnowflakeManager:
    def __init__(self, warehouse="EXPLORATION_L", database="DATABASE", schema="SCHEMA"):
        """Initialize Snowflake connection parameters."""
        self.user = os.getenv("SNOWFLAKE_USER")
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.role = os.getenv("SNOWFLAKE_ROLE")
        self.authenticator = "externalbrowser"
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.conn = None

    def connect(self):
        """Establish a connection to Snowflake."""
        if not self.conn:
            self.conn = snowflake.connector.connect(
                user=self.user,
                account=self.account,
                role=self.role,
                authenticator=self.authenticator,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )
        return self.conn

    def fetch_data(self, query):
        """Fetch data from Snowflake using the provided SQL query."""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(query)

            # Fetch data into a Pandas DataFrame
            df = pd.DataFrame.from_records(
                iter(cursor),
                columns=[col[0] for col in cursor.description]
            )
            return df

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            cursor.close()

    def use_database_and_schema(self, cursor):
        """Set the database and schema."""
        cursor.execute(f"USE DATABASE {self.database}")
        cursor.execute(f"USE SCHEMA {self.schema}")
        print(f"Using database {self.database} and schema {self.schema}")

    def table_exists(self, cursor, table_name):
        """Check if a table exists in Snowflake."""
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None

    def create_table(self, cursor, table_name, df):
        """Create the Snowflake table if it does not exist."""
        columns = ", ".join([f"{col} STRING" for col in df.columns])
        cursor.execute(f"CREATE TABLE {table_name} ({columns})")
        print(f"Table {table_name} created successfully!")

    def load_csv_to_snowflake(self, cursor, table_name, csv_file_path):
        """Load CSV data into Snowflake using PUT and COPY INTO."""
        stage_name = f"{table_name}_stage"

        # Ensure stage exists
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")

        # PUT the file into the stage
        put_sql = f"PUT file://{csv_file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        print(f"Running: {put_sql}")
        cursor.execute(put_sql)

        # COPY INTO Snowflake table
        copy_sql = f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1)
        """
        print(f"Running: {copy_sql}")
        cursor.execute(copy_sql)

        print(f"Data from {csv_file_path} loaded into {table_name} successfully!")


    def write_table(self, table_name, csv_file_path, dataset, overwrite=False):
        """Write a DataFrame to a Snowflake table with append or overwrite mode."""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            print("Snowflake connection established successfully!")

            # Set the database and schema
            self.use_database_and_schema(cursor)

            if overwrite:
                print(f"Overwriting {table_name}...")
                self.create_table(cursor, table_name, dataset)  # Drop and recreate table
            else:
                print(f"Appending to {table_name} (Creating if Needed)...")
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                table_exists = cursor.fetchall()

                if not table_exists:
                    print(f"Table {table_name} does NOT exist. Creating it now...")
                    self.create_table(cursor, table_name, dataset)  # Create if missing

            # Ensure CSV exists before loading
            if not os.path.exists(csv_file_path):
                print(f"CSV file {csv_file_path} not found! Skipping load.")
                return

            # Load CSV to Snowflake
            self.load_csv_to_snowflake(cursor, table_name, csv_file_path)

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            cursor.close()
            print(f"Data stored in {self.database}.{self.schema}.{table_name}")


    def write_csv_local(self, dataset, dataset_name, path_to_save='data', write_table_name=None):
        """Write a DataFrame to a local CSV file."""
        print(f'{dataset_name}.shape: {dataset.shape}')
        if not write_table_name:
            write_table_name = f'{dataset_name}'
        try:
            # Write DataFrame to CSV (Header included only if new table)
            csv_file_path = f'{path_to_save}/{write_table_name}.csv'
            dataset.to_csv(csv_file_path, index=False)
            print(f"CSV {write_table_name} created successfully!")
            print(csv_file_path)
            return csv_file_path

        except Exception as e:
            print(f"An error occurred: {e}")
            return None
