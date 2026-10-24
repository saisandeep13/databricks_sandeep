# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute SQL Files with Parameter Substitution
# Get table_name parameter from job
table_name = dbutils.widgets.get("table_name")

# Define the folder path containing SQL notebooks
folder_path = "/Users/saisandeep13012002@gmail.com/sql"

print(f"Starting notebook execution...")
print(f"Table name: {table_name}")
print(f"Folder path: {folder_path}")
print("=" * 50)

# List all files in the workspace folder
try:
    files = dbutils.fs.ls(f"/Workspace{folder_path}")
    
    # Filter only notebooks (files without extension or with .py/.sql/.scala/.r extensions)
    notebooks = [f for f in files if not f.name.endswith('/')]
    
    print(f"Total files found in '{folder_path}': {len(notebooks)}")
    
    if not notebooks:
        print(f"No notebooks found in {folder_path}")
    else:
        print(f"Found {len(notebooks)} notebook(s)\n")
        
        # Loop through each notebook and execute it
        for notebook in sorted(notebooks, key=lambda x: x.name):
            notebook_name = notebook.name.replace('.py', '').replace('.sql', '').replace('.scala', '').replace('.r', '')
            notebook_path = f"{folder_path}/{notebook_name}"
            
            try:
                print(f"Processing: {notebook_name}")
                
                # Execute the notebook with table_name parameter
                result = dbutils.notebook.run(notebook_path, 600, {"table_name": table_name})
                
                print(f"✓ Successfully executed: {notebook_name}")
                print(f"  Result: {result}")
                print("-" * 50)
                
            except Exception as e:
                print(f"✗ Error executing {notebook_name}: {str(e)}")
                print("-" * 50)
                # Continue with next notebook instead of failing the entire job
                continue
        
        print("\nAll notebooks processed!")
        
except Exception as e:
    print(f"Error listing notebooks in folder: {str(e)}")
    raise