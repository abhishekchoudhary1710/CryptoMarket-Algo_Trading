#!/usr/bin/env python3
"""
Script to help users set up their environment variables from .env.example template.
"""
import os
import sys
from pathlib import Path

def setup_env():
    # Get the current directory
    current_dir = Path(__file__).parent.absolute()
    
    # Check if .env.example exists
    env_example_path = current_dir / '.env.example'
    if not env_example_path.exists():
        print("Error: .env.example file not found!")
        return False
    
    # Check if .env already exists
    env_path = current_dir / '.env'
    if env_path.exists():
        response = input(".env file already exists. Do you want to overwrite it? (y/N): ")
        if response.lower() != 'y':
            print("Setup cancelled.")
            return False
    
    # Read the template
    with open(env_example_path, 'r') as f:
        template_lines = f.readlines()
    
    # Process each line and get user input
    env_vars = {}
    print("\nPlease provide values for your environment variables:")
    print("(Press Enter to use the default value if shown)\n")
    
    for line in template_lines:
        line = line.strip()
        if line and not line.startswith('#'):
            key, default_value = line.split('=', 1)
            if 'your_' in default_value or default_value == '':
                # This is a placeholder, require user input
                value = input(f"{key}=")
                while not value:
                    print(f"Value for {key} is required!")
                    value = input(f"{key}=")
            else:
                # This has a default value
                value = input(f"{key}=[{default_value}]: ")
                if not value:  # Use default if no input
                    value = default_value
            env_vars[key] = value
    
    # Write to .env file
    try:
        with open(env_path, 'w') as f:
            for line in template_lines:
                if line.strip() and not line.startswith('#'):
                    key = line.split('=', 1)[0]
                    f.write(f"{key}={env_vars[key]}\n")
                else:
                    f.write(line)
        
        print("\n.env file has been created successfully!")
        print(f"Location: {env_path}")
        return True
    
    except Exception as e:
        print(f"Error creating .env file: {e}")
        return False

def main():
    print("Setting up environment variables for Algo Trading application...\n")
    if setup_env():
        print("\nSetup completed! You can now run the application.")
    else:
        print("\nSetup failed! Please check the errors above.")

if __name__ == '__main__':
    main()