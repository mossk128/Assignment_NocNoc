import pandas as pd
import mysql.connector
import configparser

# Read the MySQL configuration from the config.ini file
config = configparser.ConfigParser()
config.read('config.ini')


# Function to establish a database connection
def connect_to_db():
    return mysql.connector.connect(
        host=config.get('mysql', 'host'),
        user=config.get('mysql', 'user'),
        password=config.get('mysql', 'password'),
        database=config.get('mysql', 'database')
    )


# Load and transform data from customer.csv
def load_customer_data():
    df = pd.read_csv('Customer.csv')
    df['created_date'] = pd.to_datetime(df['created_date'], utc='Asia/Bangkok')
    df['updated_date'] = pd.to_datetime(df['updated_date'], utc='Asia/Bangkok')
    return df

# Create the total_netsale table in the database
def create_total_netsale_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS total_netsale (
        customer_id INT PRIMARY KEY,
        first_name VARCHAR(300),
        last_name VARCHAR(300),
        total_sale_thb FLOAT,
        shipping_thb FLOAT,
        tax_thb FLOAT,
        created_date DATETIME,
        updated_date DATETIME
    );
    """
    cursor.execute(create_table_query)

# Insert customer data into the total_netsale table
def insert_customer_data(cursor, df):
    for row in df.itertuples():
        cursor.execute("INSERT INTO total_netsale VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                       (row.customer_id, row.first_name, row.last_name, row.total_sale_thb,
                        row.shipping_thb, row.tax_thb, row.created_date, row.updated_date))

# Calculate and update the sums of total_sale_thb, shipping_thb, and tax_thb
def update_sums(cursor):
    update_sums_query = """
    UPDATE total_netsale
    SET total_sale_thb = (SELECT SUM(total_sale_thb) FROM total_netsale),
        shipping_thb = (SELECT SUM(shipping_thb) FROM total_netsale),
        tax_thb = (SELECT SUM(tax_thb) FROM total_netsale);
    """
    cursor.execute(update_sums_query)

# Add created_date and updated_date columns to the total_netsale table
def add_date_columns(cursor):
    alter_table_query = """
    ALTER TABLE total_netsale
    ADD created_date DATETIME,
    ADD updated_date DATETIME;
    """
    cursor.execute(alter_table_query)

# Load product data from product.csv with name transformation
def load_product_data():
    product_df = pd.read_csv('Product.csv')
    product_df['name'] = product_df['name'].str.replace('^un^', '')
    return product_df

# Insert product data into the products table
def insert_product_data(cursor, product_df):
    for row in product_df.itertuples():
        cursor.execute("INSERT INTO products (name, price) VALUES (%s, %s)",
                       (row.name, row.price))

def main():
    db_connection = connect_to_db()
    cursor = db_connection.cursor()

    # Load and transform customer data
    customer_df = load_customer_data()

    # Create the total_netsale table and insert customer data
    create_total_netsale_table(cursor)
    insert_customer_data(cursor, customer_df)

    # Calculate and update sums
    update_sums(cursor)

    # Add date columns
    add_date_columns(cursor)

    # Load and transform product data
    product_df = load_product_data()

    # Insert product data
    insert_product_data(cursor, product_df)

    # Commit and close the database connection
    db_connection.commit()
    cursor.close()
    db_connection.close()

if __name__ == "__main__":
    main()
