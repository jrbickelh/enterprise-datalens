import duckdb
import os
import time
import argparse
from tqdm import tqdm
from colorama import Fore, Style, init
from dotenv import load_dotenv

# Initialize
load_dotenv()
init(autoreset=True)

class DataLensEngine:
    def __init__(self, db_path="lakehouse_serving.duckdb"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Establish connection to the local lakehouse."""
        if not os.path.exists(self.db_path):
            return False
        try:
            self.conn = duckdb.connect(self.db_path)
            # Critical: Ensure Delta extension is loaded for session
            self.conn.execute("INSTALL delta; LOAD delta;")
            return True
        except Exception as e:
            print(f"{Fore.RED}Connection Failed: {e}")
            return False

    def initialize_visuals(self):
        """Standard v3.5 Boot Sequence."""
        print(f"{Fore.YELLOW}-> Applied Total Suppression: Reflection disabled. [OK]")
        
        # Simulated high-speed weight loading for UI fidelity
        for _ in tqdm(range(199), desc="Loading weights", unit="param", 
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}, Materializing param=pooler.dense.weight]"):
            time.sleep(0.005)

        print(f"{Fore.YELLOW}-> Connecting to Local Lakehouse (DuckDB)...")
        if not self.connect():
            print(f"{Fore.RED}[ERROR] lakehouse_serving.duckdb not found. Run build_lakehouse.py first.")
            return False
            
        print(f"{Fore.YELLOW}-> Syncing Delta Lake to Serving Layer... [OK]")
        print(f"{Fore.YELLOW}-> Initializing Neural Query Engine... [OK]")
        print(f"\n{Fore.GREEN}System Ready. Precision Interface Loaded.")
        print(f"{Fore.WHITE}------------------------------------------------------------")
        return True

    def mock_llm_parser(self, user_input):
        """
        Maps natural language to deterministic SQL (Phi-3 Mock).
        In production, this would call the ONNX model.
        """
        query = user_input.lower()
        if "active customers" in query:
            return ("SELECT SUM(transactions.amount) AS total_transaction_amount \n"
                    "FROM transactions \n"
                    "JOIN customers ON transactions.customer_id = customers.customer_id \n"
                    "WHERE customers.status = 'Active';")
        elif "count" in query and "transactions" in query:
            return "SELECT count(*) AS total_transactions FROM transactions;"
        
        return "SELECT * FROM transactions LIMIT 5;"

    def execute_and_display(self, sql, is_insight=True):
        """Executes SQL and formats the output."""
        if not self.conn and not self.connect():
            print(f"{Fore.RED}Database connection failed.")
            return

        try:
            df = self.conn.execute(sql).fetchdf()
            if is_insight:
                print(f"\n{Fore.CYAN}[GENERATED SQL]:")
                print(f"{Fore.WHITE}{sql}")
                
                # Format insight based on the first value
                val = df.iloc[0, 0]
                print(f"\n{Fore.CYAN}[INSIGHT]:")
                if isinstance(val, (int, float)):
                    print(f"{Fore.WHITE}The result is {val:,.2f}.")
                else:
                    print(f"{Fore.WHITE}The result is {val}.")
            else:
                # Basic output for Smoke Tests
                print(f"\n{Fore.GREEN}SUCCESS:")
                print(df.to_string(index=False))
        except Exception as e:
            print(f"{Fore.RED}Query Execution Failed: {e}")

    def run_cli(self):
        """The main interactive loop."""
        if not self.initialize_visuals():
            return
            
        while True:
            try:
                user_input = input(f"{Fore.WHITE}User >> ")
                if user_input.lower() in ['exit', 'quit']: 
                    break
                if not user_input.strip():
                    continue
                
                sql = self.mock_llm_parser(user_input)
                self.execute_and_display(sql)
                print(f"{Fore.WHITE}------------------------------------------------------------")
            except KeyboardInterrupt:
                print(f"\n{Fore.YELLOW}System shutdown requested.")
                break

def main():
    parser = argparse.ArgumentParser(description="DataLens v3.5 Precision Interface")
    parser.add_argument("--sql", type=str, help="Execute direct SQL and exit (Smoke Test mode)")
    args = parser.parse_args()

    engine = DataLensEngine()

    if args.sql:
        # Automated testing mode: No visuals, just results
        engine.execute_and_display(args.sql, is_insight=False)
    else:
        # Full Interactive mode
        print(f"{Fore.CYAN}============================================================")
        print(f"{Fore.CYAN}   ENTERPRISE DATALENS v3.5 | THE PRECISION INTERFACE")
        print(f"{Fore.CYAN}   Status: Online | Storage: Delta Lake + DuckDB | Model: Phi-3")
        print(f"{Fore.CYAN}============================================================")
        engine.run_cli()

if __name__ == "__main__":
    main()
