# tests/test_registry.py
"""
Tests for datasets/registry.py (Item 1 — Dataset Registry)
รันได้โดยไม่ต้องมี Hive/Spark/Docker
"""

import pytest
from pathlib import Path
import yaml

from datasets.registry import (
    load_dataset,
    build_category_mapping_text,
    build_schema_prompt,
    DatasetConfig,
)


# ── Fixtures ─────────────────────────────────────────────────

MINIMAL_YAML = {
    "dataset": "test_ds",
    "owner": "test-owner",
    "paths": {
        "raw": "/datalake/raw/test_ds",
        "staging": "/datalake/staging/test_ds",
        "curated": "/datalake/curated/test_ds",
        "versions": "/datalake/versions/test_ds",
        "trash": "/datalake/trash",
    },
    "tables": {
        "database": "default",
        "staging": "test_ds_wide",
        "curated": "test_ds_long",
    },
    "pipeline": {
        "critical_columns": ["date", "details"],
        "partition_by": "year",
    },
    "schema": [
        {
            "name": "date",
            "type": "STRING",
            "thai_name": "เดือน",
            "description": "เดือนของรายการ",
            "reserved_keyword": True,
        },
        {
            "name": "amount",
            "type": "DECIMAL",
            "thai_name": "จำนวนเงิน",
            "description": "จำนวนเงิน (บาท)",
        },
        {
            "name": "year",
            "type": "INT",
            "thai_name": "ปี",
            "description": "ปีของรายการ",
            "partition": True,
        },
    ],
    "category_mapping": {
        "expense_budget": ["ค่าใช้สอย", "งบประจำ(ค่าใช้สอย)"],
        "utilities": ["ค่าสาธารณูปโภค"],
    },
    "nlp_rules": [
        "`date` เป็น reserved keyword ต้องใส่ backtick ทุกครั้ง",
        "'remaining' → ห้าม SUM เด็ดขาด",
    ],
    "example_queries": [
        {
            "q": "ยอดใช้จ่ายรวม",
            "sql": "SELECT SUM(amount) FROM test_ds_long WHERE details = 'spent'",
        }
    ],
}


@pytest.fixture
def yaml_dir(tmp_path):
    """สร้าง temp dir พร้อม test.yaml และ test_ds.yaml"""
    yaml_path = tmp_path / "test_ds.yaml"
    yaml_path.write_text(yaml.dump(MINIMAL_YAML, allow_unicode=True), encoding="utf-8")
    return tmp_path


@pytest.fixture
def finance_yaml_dir():
    """ชี้ไปที่ datasets/ จริงในโปรเจค"""
    return Path(__file__).parent.parent / "datasets"


# ── load_dataset ─────────────────────────────────────────────

class TestLoadDataset:

    def test_load_success(self, yaml_dir):
        ds = load_dataset("test_ds", datasets_dir=yaml_dir)
        assert isinstance(ds, DatasetConfig)
        assert ds.dataset == "test_ds"
        assert ds.owner == "test-owner"

    def test_load_not_found_raises(self, yaml_dir):
        with pytest.raises(FileNotFoundError, match="not found"):
            load_dataset("nonexistent", datasets_dir=yaml_dir)

    def test_load_finance_yaml_exists(self, finance_yaml_dir):
        """finance.yaml ต้องโหลดได้จริง"""
        if not finance_yaml_dir.exists():
            pytest.skip("datasets/ directory not found")
        ds = load_dataset("finance_itsc", datasets_dir=finance_yaml_dir)
        assert ds.dataset == "finance_itsc"

    def test_shortname_alias(self, tmp_path):
        """'finance' ควร fallback ไปที่ finance.yaml ได้"""
        yaml_path = tmp_path / "finance.yaml"
        data = dict(MINIMAL_YAML, dataset="finance_itsc")
        yaml_path.write_text(yaml.dump(data, allow_unicode=True), encoding="utf-8")
        ds = load_dataset("finance_itsc", datasets_dir=tmp_path)
        assert ds.dataset == "finance_itsc"


# ── DatasetConfig properties ─────────────────────────────────

class TestDatasetConfigProperties:

    @pytest.fixture(autouse=True)
    def setup(self, yaml_dir):
        self.ds = load_dataset("test_ds", datasets_dir=yaml_dir)

    def test_curated_table(self):
        assert self.ds.curated_table == "test_ds_long"

    def test_staging_table(self):
        assert self.ds.staging_table == "test_ds_wide"

    def test_database(self):
        assert self.ds.database == "default"

    def test_critical_columns(self):
        assert "date" in self.ds.critical_columns
        assert "details" in self.ds.critical_columns

    def test_col_lookup_found(self):
        col = self.ds.col("date")
        assert col is not None
        assert col.type == "STRING"
        assert col.reserved_keyword is True

    def test_col_lookup_not_found(self):
        assert self.ds.col("nonexistent") is None


# ── ColumnDef ────────────────────────────────────────────────

class TestColumnDef:

    @pytest.fixture(autouse=True)
    def setup(self, yaml_dir):
        self.ds = load_dataset("test_ds", datasets_dir=yaml_dir)

    def test_schema_length(self):
        assert len(self.ds.schema) == 3

    def test_reserved_keyword_flag(self):
        date_col = self.ds.col("date")
        assert date_col.reserved_keyword is True

    def test_partition_flag(self):
        year_col = self.ds.col("year")
        assert year_col.partition is True

    def test_normal_column_defaults(self):
        amount_col = self.ds.col("amount")
        assert amount_col.reserved_keyword is False
        assert amount_col.partition is False
        assert amount_col.allowed_values == []


# ── build_category_mapping_text ──────────────────────────────

class TestBuildCategoryMappingText:

    @pytest.fixture(autouse=True)
    def setup(self, yaml_dir):
        self.ds = load_dataset("test_ds", datasets_dir=yaml_dir)
        self.text = build_category_mapping_text(self.ds)

    def test_contains_column_names(self):
        assert "expense_budget" in self.text
        assert "utilities" in self.text

    def test_contains_thai_aliases(self):
        assert "ค่าใช้สอย" in self.text
        assert "ค่าสาธารณูปโภค" in self.text

    def test_mapping_header_present(self):
        assert "Mapping" in self.text

    def test_empty_mapping(self, yaml_dir):
        import copy
        data = copy.deepcopy(MINIMAL_YAML)
        data["category_mapping"] = {}
        p = yaml_dir / "empty_cat.yaml"
        p.write_text(yaml.dump(data, allow_unicode=True), encoding="utf-8")
        ds2 = load_dataset("empty_cat", datasets_dir=yaml_dir)
        text = build_category_mapping_text(ds2)
        assert "Mapping" in text  # header ยังอยู่


# ── build_schema_prompt ──────────────────────────────────────

class TestBuildSchemaPrompt:

    @pytest.fixture(autouse=True)
    def setup(self, yaml_dir):
        self.ds = load_dataset("test_ds", datasets_dir=yaml_dir)
        self.prompt = build_schema_prompt(self.ds)

    def test_contains_table_name(self):
        assert "test_ds_long" in self.prompt

    def test_reserved_keyword_backtick(self):
        # date ต้องถูก wrap ด้วย backtick ใน prompt
        assert "`date`" in self.prompt

    def test_contains_nlp_rules(self):
        assert "reserved keyword" in self.prompt
        assert "remaining" in self.prompt

    def test_contains_example_queries(self):
        assert "ยอดใช้จ่ายรวม" in self.prompt
        assert "SELECT SUM" in self.prompt

    def test_contains_category_mapping(self):
        assert "expense_budget" in self.prompt
        assert "ค่าใช้สอย" in self.prompt

    def test_no_example_queries(self, yaml_dir):
        import copy
        data = copy.deepcopy(MINIMAL_YAML)
        data["example_queries"] = []
        p = yaml_dir / "no_ex.yaml"
        p.write_text(yaml.dump(data, allow_unicode=True), encoding="utf-8")
        ds2 = load_dataset("no_ex", datasets_dir=yaml_dir)
        prompt = build_schema_prompt(ds2)
        # ต้องไม่ crash และยังมี schema อยู่
        assert "test_ds_long" in prompt


# ── finance.yaml จริง (integration) ─────────────────────────

class TestFinanceYamlIntegration:
    """ทดสอบ finance.yaml จริง — skip ถ้าไม่พบไฟล์"""

    @pytest.fixture(autouse=True)
    def setup(self):
        finance_dir = Path(__file__).parent.parent / "datasets"
        if not (finance_dir / "finance.yaml").exists():
            pytest.skip("datasets/finance.yaml not found")
        self.ds = load_dataset("finance_itsc", datasets_dir=finance_dir)

    def test_required_fields(self):
        assert self.ds.dataset == "finance_itsc"
        assert self.ds.curated_table == "finance_itsc_long"
        assert self.ds.database == "default"

    def test_critical_columns_present(self):
        assert "date" in self.ds.critical_columns
        assert "details" in self.ds.critical_columns

    def test_schema_has_required_columns(self):
        names = [c.name for c in self.ds.schema]
        for required in ["date", "details", "category", "amount", "year"]:
            assert required in names, f"column '{required}' missing from schema"

    def test_date_is_reserved_keyword(self):
        date_col = self.ds.col("date")
        assert date_col is not None
        assert date_col.reserved_keyword is True

    def test_year_is_partition(self):
        year_col = self.ds.col("year")
        assert year_col is not None
        assert year_col.partition is True

    def test_category_mapping_not_empty(self):
        assert len(self.ds.category_mapping) > 0

    def test_nlp_rules_not_empty(self):
        assert len(self.ds.nlp_rules) > 0

    def test_example_queries_not_empty(self):
        assert len(self.ds.example_queries) > 0

    def test_prompt_builds_without_error(self):
        prompt = build_schema_prompt(self.ds)
        assert len(prompt) > 100
        assert "`date`" in prompt
        assert "finance_itsc_long" in prompt