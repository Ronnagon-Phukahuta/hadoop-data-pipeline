import sys
from jobs.engine.run_pipeline import _resolve_dataset_name
from datasets.registry import load_dataset


sys.argv = ['run_pipeline.py', 'finance_itsc']
print('from CLI:', _resolve_dataset_name())

sys.argv = ['run_pipeline.py']
print('default:', _resolve_dataset_name())


ds = load_dataset('finance_itsc')
print('id_columns:', ds.id_columns)
print('exclude_columns:', ds.exclude_columns)
print('partition_by:', ds.partition_by)