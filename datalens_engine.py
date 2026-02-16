import os
import sys
import logging
import warnings
from dotenv import load_dotenv # <--- NEW IMPORT

# Load environment variables from .env file
load_dotenv() # <--- LOADS YOUR TOKEN

# --- 0. SILENCE & UX CONFIG ---
def configure_environment():
    # 1. Disable HuggingFace Progress Bars
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    os.environ["TRANSFORMERS_VERBOSITY"] = "error"
    os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    
    # Note: We don't need to manually set HF_TOKEN here anymore.
    # load_dotenv() injected it into os.environ for us!

    # 2. Filter Warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    # 3. Silence Loggers
    logging.getLogger("transformers").setLevel(logging.ERROR)
    logging.getLogger("huggingface_hub").setLevel(logging.ERROR)

configure_environment()
# ... rest of the script remains the same ...

# -------------------------------------------------------------

import yaml
from sqlalchemy import create_engine
from llama_index.llms.ollama import Ollama
from llama_index.core import SQLDatabase, Settings
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.embeddings.huggingface import HuggingFaceEmbedding

# 1. SETUP DISPLAY
os.system('cls' if os.name == 'nt' else 'clear') # Clear terminal for fresh start
print("="*60)
print("   ENTERPRISE DATALENS | SEMANTIC AI ENGINE")
print("   Status: Online | Governance: Active | Model: Phi-3 (Local)")
print("="*60 + "\n")

DB_URI = "sqlite:///banking_warehouse.db"

# 2. CONFIGURATION
# Context limited to 4096 to prevent RAM overflow on 3GB GPU
Settings.llm = Ollama(
    model="phi3", 
    request_timeout=360.0, 
    additional_kwargs={"num_ctx": 4096}
) 

# Embedding Model
Settings.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")

# 3. LOAD SEMANTIC LAYER
def load_semantic_layer(file_path="metadata.yaml"):
    try:
        with open(file_path, "r") as f:
            metadata = yaml.safe_load(f)
        
        context_str = "Use this data dictionary to interpret the schema:\n"
        for table, details in metadata['tables'].items():
            context_str += f"\nTABLE: {table}\nDESC: {details['description']}\n"
            for col, desc in details['columns'].items():
                context_str += f"- COL {col}: {desc}\n"
        return context_str
    except FileNotFoundError:
        print("Error: metadata.yaml not found. Semantic Layer inactive.")
        return ""

# 4. INITIALIZATION
try:
    print("-> Connecting to Secure Data Warehouse...", end=" ")
    engine = create_engine(DB_URI)
    sql_database = SQLDatabase(engine, include_tables=["customers", "accounts", "transactions", "risk_profile"])
    print("[OK]")

    print("-> Loading Semantic Knowledge Graph...", end=" ")
    semantic_context = load_semantic_layer()
    print("[OK]")

    print("-> Initializing Neural Query Engine...", end=" ")
    query_engine = NLSQLTableQueryEngine(
        sql_database=sql_database,
        tables=["customers", "accounts", "transactions", "risk_profile"],
        context_query_kwargs={"banking_warehouse": semantic_context}
    )
    print("[OK]\n")

except Exception as e:
    print(f"\n[CRITICAL ERROR]: {e}")
    sys.exit(1)

# 5. RUN LOOP
if __name__ == "__main__":
    print("System Ready. Enter your query below (Type 'exit' to quit).")
    print("-" * 60)
    
    while True:
        user_q = input("User >> ")
        if user_q.lower() == 'exit': break
        
        try:
            print("Agent is thinking...", end="\r") 
            response = query_engine.query(user_q)
            
            # Clear "Thinking" line
            print(" " * 50, end="\r")
            
            # Formatted Output
            print(f"\n[GENERATED SQL]:\n{response.metadata['sql_query']}")
            print(f"\n[INSIGHT]:\n{response}\n")
            print("-" * 60)
            
        except Exception as e:
            print(f"\n[ERROR]: {e}\n")
