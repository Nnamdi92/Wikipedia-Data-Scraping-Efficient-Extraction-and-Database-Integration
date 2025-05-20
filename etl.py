import subprocess
import os

def run_notebook(notebook_path):
    print(f"🚀 Running notebook: {notebook_path}")
    result = subprocess.run([
        "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute", notebook_path,
        "--output", os.devnull
    ], check=True)
    print(f"✅ Finished: {notebook_path}")

if __name__ == "__main__":
    try:
        run_notebook("extract.ipynb")
        run_notebook("cleaning.ipynb")
        run_notebook("postgres-load.ipynb")
        print("🎉 ETL pipeline completed successfully.")
    except subprocess.CalledProcessError as e:
        print("❌ ETL pipeline failed:", e)
