import sqlite3
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)  # Reproducible results

def create_schema(cursor):
    # 1. Customers (Demographics)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        segment TEXT, -- 'Wealth', 'Retail', 'Student'
        joined_date DATE
    )''')

    # 2. Accounts (Holdings)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS accounts (
        account_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        account_type TEXT, -- 'Checking', 'Savings', 'HELOC'
        balance REAL,
        status TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )''')

    # 3. Transactions (High Volume)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        trans_id INTEGER PRIMARY KEY,
        account_id INTEGER,
        trans_date TIMESTAMP,
        amount REAL,
        merchant TEXT,
        category TEXT, 
        is_suspicious BOOLEAN, -- 1=True, 0=False
        FOREIGN KEY (account_id) REFERENCES accounts(account_id)
    )''')

    # 4. Risk Profile (Data Science)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS risk_profile (
        customer_id INTEGER PRIMARY KEY,
        churn_risk_score INTEGER, -- 0-100
        credit_limit REAL,
        last_audit_date DATE,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )''')

def generate_data():
    conn = sqlite3.connect('banking_warehouse.db')
    cursor = conn.cursor()
    create_schema(cursor)
    
    print("Generating 100 customers and their banking history...")
    
    customers = []
    accounts = []
    transactions = []
    risks = []

    for i in range(100):
        cid = i + 1
        # Customer
        customers.append((cid, fake.first_name(), fake.last_name(), fake.email(), 
                          random.choice(['Retail', 'Wealth', 'Student']), fake.date_this_decade()))
        
        # Risk Profile
        risks.append((cid, random.randint(1, 100), random.choice([5000, 10000, 50000]), fake.date_this_year()))

        # Accounts (1-2 per customer)
        for _ in range(random.randint(1, 2)):
            aid = len(accounts) + 1
            accounts.append((aid, cid, random.choice(['Checking', 'Savings']), round(random.uniform(100, 50000), 2), 'Active'))
            
            # Transactions (10-30 per account)
            for _ in range(random.randint(10, 30)):
                tid = len(transactions) + 1
                transactions.append((
                    tid, aid, fake.date_time_this_year(), round(random.uniform(-500, 500), 2),
                    fake.company(), random.choice(['Groceries', 'Tech', 'Dining', 'Salary']), 
                    random.choice([True, False, False, False, False]) # 20% suspicious
                ))

    cursor.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", customers)
    cursor.executemany("INSERT INTO accounts VALUES (?,?,?,?,?)", accounts)
    cursor.executemany("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)", transactions)
    cursor.executemany("INSERT INTO risk_profile VALUES (?,?,?,?)", risks)

    conn.commit()
    conn.close()
    print("Database 'banking_warehouse.db' created successfully.")

if __name__ == "__main__":
    generate_data()
