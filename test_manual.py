from jobs.datasets.registry import load_dataset
from jobs.quality_rules.finance_rules import FinanceQualityRules

ds = load_dataset('finance_itsc')
print('required_columns:', ds.required_columns)
print('amount_columns count:', len(ds.amount_columns))
print('critical_columns:', ds.critical_columns)

ds = load_dataset('finance_itsc')
rules = FinanceQualityRules(ds)
print('rules.ds.dataset:', rules.ds.dataset)
print('required_columns:', rules._required_columns)
print('amount_columns count:', len(rules._amount_columns))