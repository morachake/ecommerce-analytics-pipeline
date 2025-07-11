#!/usr/bin/env python3
"""
Generate Fernet key for Airflow configuration
"""

try:
    from cryptography.fernet import Fernet
    
    # Generate a new Fernet key
    fernet_key = Fernet.generate_key().decode()
    
    print("🔑 Generated Fernet Key:")
    print(f"AIRFLOW__CORE__FERNET_KEY={fernet_key}")
    print()
    print("Copy this key and replace 'your-fernet-key-here-replace-with-generated-key' in your .env file")
    
except ImportError:
    print("❌ cryptography library not found. Installing...")
    import subprocess
    import sys
    
    subprocess.check_call([sys.executable, "-m", "pip", "install", "cryptography"])
    
    from cryptography.fernet import Fernet
    fernet_key = Fernet.generate_key().decode()
    
    print("🔑 Generated Fernet Key:")
    print(f"AIRFLOW__CORE__FERNET_KEY={fernet_key}")
    print()
    print("Copy this key and replace 'your-fernet-key-here-replace-with-generated-key' in your .env file")