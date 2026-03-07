from datasets.registry import load_dataset, build_schema_prompt
from dashboard.config import get_table_schema_prompt
from dashboard.config import get_dataset, GPT_MODEL, HISTORY_FILE

print('Testing dataset loading and prompt building...\n')
ds = load_dataset('finance_itsc')
print('dataset:', ds.dataset)
print('table:', ds.curated_table)
print('columns:', [c.name for c in ds.schema])
print('categories:', len(ds.category_mapping))

print('TABLE_SCHEMA PROMPT:')
print(get_table_schema_prompt())

print('GPT CONFIG:')
ds = get_dataset()
print('GPT_MODEL:', GPT_MODEL)
print('HISTORY_FILE:', HISTORY_FILE)
print('HIVE_DB:', ds.database)