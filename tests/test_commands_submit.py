"""Tests for the submit CLI → Step Functions input payload shape.

We don't exercise AWS itself — just verify (a) the input JSON the CLI
constructs matches the state machine's schema, and (b) ARN resolution
prefers env > pulumi in the right order.
"""

from __future__ import annotations

import json
import os
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from poochon_backtest_data.commands import submit as submit_cmd


def _args(**kwargs) -> Namespace:
    defaults = {
        "command": "submit",
        "venue": None,
        "market_type": "perp",
        "instrument": "BTC",
        "series": "btc-updown-5m",
        "start_date": "2026-02-19",
        "end_date": "2026-02-19",
        "depth": 20,
        "outcomes": "both",
        "stack": "dev",
        "state_machine_arn": None,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


class TestArnResolution:
    def test_explicit_arn_wins(self):
        arn = submit_cmd.resolve_state_machine_arn("dev", explicit="arn:aws:states:::explicit")
        assert arn == "arn:aws:states:::explicit"

    def test_env_var_is_next_priority(self, monkeypatch):
        monkeypatch.setenv("POOCHON_INGESTION_STATE_MACHINE_ARN", "arn:aws:states:::env")
        arn = submit_cmd.resolve_state_machine_arn("dev")
        assert arn == "arn:aws:states:::env"

    def test_falls_back_to_pulumi(self, monkeypatch):
        monkeypatch.delenv("POOCHON_INGESTION_STATE_MACHINE_ARN", raising=False)
        with patch.object(submit_cmd, "_pulumi_output", return_value="arn:aws:states:::pulumi") as mock_pulumi:
            arn = submit_cmd.resolve_state_machine_arn("dev")
        assert arn == "arn:aws:states:::pulumi"
        mock_pulumi.assert_called_once_with("dev", "ingestion_state_machine_arn")


class TestPayloadShape:
    def test_hyperliquid_payload(self, monkeypatch):
        monkeypatch.setenv("POOCHON_INGESTION_STATE_MACHINE_ARN", "arn:aws:states:::test")
        captured = {}
        session = MagicMock()
        sfn = MagicMock()
        sfn.start_execution.return_value = {"executionArn": "arn:aws:states:exec:foo"}
        session.client.return_value = sfn

        def capture_start(**kwargs):
            captured.update(kwargs)
            return {"executionArn": "arn:aws:states:exec:foo"}
        sfn.start_execution.side_effect = capture_start

        with patch.object(submit_cmd, "boto3_session", return_value=session):
            exit_code = submit_cmd.handle(_args(venue="hyperliquid"))
        assert exit_code == 0
        payload = json.loads(captured["input"])
        assert payload == {
            "venue": "hyperliquid",
            "market_type": "perp",
            "instrument": "BTC",
            "start_date": "2026-02-19",
            "end_date": "2026-02-19",
            "depth": 20,
        }
        assert captured["stateMachineArn"] == "arn:aws:states:::test"
        assert captured["name"].startswith("hl-BTC-2026-02-19-2026-02-19")

    def test_polymarket_payload(self, monkeypatch):
        monkeypatch.setenv("POOCHON_INGESTION_STATE_MACHINE_ARN", "arn:aws:states:::test")
        captured = {}
        session = MagicMock()
        sfn = MagicMock()
        def capture_start(**kwargs):
            captured.update(kwargs)
            return {"executionArn": "arn:aws:states:exec:bar"}
        sfn.start_execution.side_effect = capture_start
        session.client.return_value = sfn

        with patch.object(submit_cmd, "boto3_session", return_value=session):
            submit_cmd.handle(_args(venue="polymarket", depth=5))
        payload = json.loads(captured["input"])
        assert payload == {
            "venue": "polymarket",
            "series": "btc-updown-5m",
            "start_date": "2026-02-19",
            "end_date": "2026-02-19",
            "outcomes": "both",
            "depth": 5,
        }


class TestExecutionName:
    def test_sanitizes_unsafe_chars(self):
        name = submit_cmd._execution_name("hl-BTC/ETH:2026-02-19")
        assert "/" not in name
        assert ":" not in name

    def test_caps_at_80_chars(self):
        name = submit_cmd._execution_name("x" * 200)
        assert len(name) <= 80
