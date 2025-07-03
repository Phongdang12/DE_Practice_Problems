import psycopg2
import pandas as pd


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    # your code here
    cur = conn.cursor()
    
    # Create tables 
    cur.execute("""
       CREATE TABLE IF NOT EXISTS accounts (
                customer_id INT PRIMARY KEY,
                full_name VARCHAR(100),
                address_1 VARCHAR(50),
                address_2 VARCHAR(50),
                city VARCHAR(50),
                state VARCHAR(50),
                zip_code int,
                join_date DATE,
                last_login TIMESTAMP default CURRENT_TIMESTAMP
                );
 """)
    cur.execute("""
       CREATE TABLE IF NOT EXISTS products(
                product_id INT PRIMARY KEY,
                product_code INT,
                product_description VARCHAR(50),
                last_login TIMESTAMP default CURRENT_TIMESTAMP
                );
                """)
    cur.execute("""
       CREATE TABLE IF NOT EXISTS transactions(
                transaction_id VARCHAR(100) PRIMARY KEY,
                transaction_date DATE,
                product_id INT,
                product_code INT,
                product_description VARCHAR(50),
                quantity INT,
                account_id INT,
                last_login TIMESTAMP default CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
                );
                """)

    # Create indexes with correct column names for faster queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_name ON accounts(full_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_code ON products(product_code);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);")
    
    # Insert and clean accounts data accounts.csv
    df = pd.read_csv("data/accounts.csv")
    df.columns = df.columns.str.strip()

    df["full_name"] = df["first_name"] + " " + df["last_name"]
    df["full_name"] = df["full_name"].str.title()
    df.drop(columns=["first_name","last_name"], inplace=True)
    
    for index, row in df.iterrows():
        cur.execute("""
           INSERT INTO accounts (customer_id, full_name, address_1, address_2, city, state, zip_code, join_date) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   """, (
                        row['customer_id'],
                        row['full_name'],
                        row['address_1'],
                        row['address_2'],
                        row['city'],
                        row['state'],
                        row['zip_code'],
                        row['join_date']
                   ))
    
    # Insert and clean accounts data product.csv
    df = pd.read_csv("data/products.csv")
    df.columns = df.columns.str.strip()

    for index, row in df.iterrows():
        cur.execute("""
           INSERT INTO products (product_id, product_code, product_description) 
           VALUES (%s, %s, %s)
                   """, (
                        row['product_id'],
                        row['product_code'],
                        row['product_description']
                   ))

    # Insert and clean accounts data transactions.csv
    df = pd.read_csv("data/transactions.csv")
    df.columns = df.columns.str.strip()

    for index, row in df.iterrows():
        cur.execute("""
           INSERT INTO transactions (transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id) 
           VALUES (%s, %s, %s, %s, %s, %s, %s)
                   """, (
                        row['transaction_id'],
                        row['transaction_date'],
                        row['product_id'],
                        row['product_code'],
                        row['product_description'],
                        row['quantity'],
                        row['account_id']
                   ))
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("Database tables and indexes created successfully!")
    
    
if __name__ == "__main__":
    main()
