from pathlib import Path

import pytest

from emergency_processing import config


def test_relative_path_resolves_from_application_root(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setenv("TEST_DATASET_PATH", "fixtures/calls.jsonl")

    resolved = config.env_path("TEST_DATASET_PATH", "unused.jsonl")

    assert resolved == tmp_path / "fixtures" / "calls.jsonl"


def test_environment_value_overrides_numeric_default(monkeypatch):
    monkeypatch.setenv("TEST_CONSUMER_BATCH", "7")

    assert config.env_int("TEST_CONSUMER_BATCH", 3) == 7


def test_required_setting_error_names_missing_variable(monkeypatch):
    setting_name = "TEST_REQUIRED_SETTING"
    monkeypatch.delenv(setting_name, raising=False)

    with pytest.raises(RuntimeError, match=setting_name):
        config.require_env(setting_name)
