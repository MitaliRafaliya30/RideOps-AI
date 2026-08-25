import json
import sys
from pathlib import Path

# Read the notebook
with open('01_HVFHV_Data_Profiling.ipynb', 'r') as f:
    notebook = json.load(f)

# Create shared execution context to maintain state
exec_context = {}

# Execute code cells in order
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code':
        source = ''.join(cell['source'])
        if source.strip():
            print(f"\n{'='*60}")
            print(f"Cell {i}: Executing code")
            print('='*60)
            print("Code:")
            print(source)
            print("\nOutput:")
            try:
                exec(source, exec_context)
            except Exception as e:
                print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
