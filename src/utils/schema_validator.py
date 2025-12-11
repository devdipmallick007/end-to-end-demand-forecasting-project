import pandas as pd
import yaml
import re
from utils.logger import logger


class SchemaValidator:
    """
    Validates a dataframe using rules defined in a YAML schema file.
    Supports dtype, min/max, required, unique, allowed_values, and regex validation.
    """

    def __init__(self, schema_path: str):
        logger.info(f"Loading schema from: {schema_path}")

        with open(schema_path, "r") as f:
            self.schema = yaml.safe_load(f)

        self.columns_schema = self.schema.get("columns", {})
        self.allow_extra_columns = self.schema.get("allow_extra_columns", False)


    def validate(self, df: pd.DataFrame):
        logger.info("Running schema validation...")

        self._check_required_columns(df)
        self._check_unexpected_columns(df)
        self._validate_column_types(df)
        self._validate_value_constraints(df)
        self._validate_unique(df)
        self._validate_allowed_values(df)
        self._validate_regex(df)

        logger.info("Schema validation passed successfully.")
        return df


    def _check_required_columns(self, df):
        for col, rules in self.columns_schema.items():
            if rules.get("required", False) and col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
        logger.info("✓ Required column check passed")


    def _check_unexpected_columns(self, df):
        if self.allow_extra_columns:
            return
        unexpected = set(df.columns) - set(self.columns_schema.keys())
        if unexpected:
            raise ValueError(f"Unexpected columns found: {unexpected}")
        logger.info("✓ Unexpected column check passed")

    def _validate_column_types(self, df):
        for col, rules in self.columns_schema.items():
            if col not in df.columns:
                continue

            expected_type = rules.get("dtype", None)
            if expected_type is None:
                continue

            try:
                if expected_type == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="raise")
                else:
                    df[col] = df[col].astype(expected_type)
            except Exception:
                raise TypeError(f"Column {col} has invalid dtype. Expected: {expected_type}")

        logger.info("✓ Data type validation passed")


    def _validate_value_constraints(self, df):
        for col, rules in self.columns_schema.items():
            if col not in df.columns:
                continue
            if "min" in rules and df[col].min() < rules["min"]:
                raise ValueError(f"Column {col} has values below minimum {rules['min']}")
            if "max" in rules and df[col].max() > rules["max"]:
                raise ValueError(f"Column {col} has values above maximum {rules['max']}")
        logger.info("✓ Value constraint check passed")

    def _validate_unique(self, df):
        for col, rules in self.columns_schema.items():
            if col not in df.columns:
                continue
            if rules.get("unique", False) and not df[col].is_unique:
                raise ValueError(f"Column {col} must be unique but contains duplicates")
        logger.info("✓ Unique constraint check passed")

    def _validate_allowed_values(self, df):
        for col, rules in self.columns_schema.items():
            if col not in df.columns:
                continue
            allowed_values = rules.get("allowed_values", None)
            if allowed_values is not None:
                invalid = set(df[col].dropna()) - set(allowed_values)
                if invalid:
                    raise ValueError(f"Column {col} contains invalid values: {invalid}")
        logger.info("✓ Allowed values check passed")

    def _validate_regex(self, df):
        for col, rules in self.columns_schema.items():
            if col not in df.columns:
                continue
            pattern = rules.get("regex", None)
            if pattern is not None:
                invalid_rows = df[~df[col].astype(str).str.match(pattern)]
                if not invalid_rows.empty:
                    raise ValueError(f"Column {col} has values not matching regex '{pattern}': {invalid_rows[col].tolist()}")
        logger.info("✓ Regex pattern check passed")
