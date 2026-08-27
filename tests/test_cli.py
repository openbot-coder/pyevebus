"""Client CLI 测试 — evebus (客户端管理工具)"""
import json
import os
import tempfile
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from evebus.cli import cli, _api_get, _api_post, _api_delete


@pytest.fixture
def runner():
    return CliRunner()


# ═══════════════════════════════════════
#  基础命令
# ═══════════════════════════════════════

class TestBasicCommands:

    def test_help(self, runner):
        r = runner.invoke(cli, ["--help"])
        assert r.exit_code == 0
        assert "EveBus Control" in r.output

    def test_version(self, runner):
        r = runner.invoke(cli, ["--version"])
        assert r.exit_code == 0
        assert "0.3.0" in r.output

    def test_sources_help(self, runner):
        r = runner.invoke(cli, ["sources", "--help"])
        assert r.exit_code == 0

    def test_executors_help(self, runner):
        r = runner.invoke(cli, ["executors", "--help"])
        assert r.exit_code == 0

    def test_plugins_help(self, runner):
        r = runner.invoke(cli, ["plugins", "--help"])
        assert r.exit_code == 0

    def test_no_serve_command(self, runner):
        """serve 不在客户端 CLI 中"""
        r = runner.invoke(cli, ["serve"])
        assert r.exit_code != 0

    def test_no_run_command(self, runner):
        """run 不在客户端 CLI 中"""
        r = runner.invoke(cli, ["run"])
        assert r.exit_code != 0


# ═══════════════════════════════════════
#  发射命令
# ═══════════════════════════════════════

class TestEmitCommand:

    @patch("urllib.request.urlopen")
    def test_emit_ok(self, mock_urlopen, runner):
        resp = MagicMock()
        resp.read.return_value = b'{"ok": true, "topic": "test.a", "handled": true}'
        mock_urlopen.return_value = resp
        r = runner.invoke(cli, ["emit", "test.a", "-d", '{"x": 1}'])
        assert r.exit_code == 0
        assert "已发射" in r.output

    @patch("urllib.request.urlopen")
    def test_emit_not_handled(self, mock_urlopen, runner):
        resp = MagicMock()
        resp.read.return_value = b'{"ok": true, "topic": "test.a", "handled": false}'
        mock_urlopen.return_value = resp
        r = runner.invoke(cli, ["emit", "test.a"])
        assert r.exit_code == 0
        assert "无 handler" in r.output

    def test_emit_bad_json(self, runner):
        r = runner.invoke(cli, ["emit", "test.a", "-d", "not json"])
        assert r.exit_code != 0

    @patch("urllib.request.urlopen")
    def test_emit_connection_error(self, mock_urlopen, runner):
        mock_urlopen.side_effect = ConnectionError("refused")
        r = runner.invoke(cli, ["emit", "test.a"])
        assert r.exit_code != 0


# ═══════════════════════════════════════
#  状态命令
# ═══════════════════════════════════════

class TestStatusCommand:

    @patch("urllib.request.urlopen")
    def test_status_ok(self, mock_urlopen, runner):
        resp = MagicMock()
        resp.read.return_value = json.dumps({
            "status": "ok",
            "stats": {
                "sources": {"count": 1, "running": 1, "names": ["t1"]},
                "executors": {"count": 2, "names": ["e1", "e2"], "total_executed": 10, "total_errors": 0},
                "plugins": {"count": 0, "names": []},
                "handlers": {"count": 3, "patterns": ["a.*"]},
                "pending_tasks": 0,
            },
        }).encode()
        mock_urlopen.return_value = resp
        r = runner.invoke(cli, ["status"])
        assert r.exit_code == 0
        assert "事件源" in r.output
        assert "执行器" in r.output

    @patch("urllib.request.urlopen")
    def test_status_connection_error(self, mock_urlopen, runner):
        mock_urlopen.side_effect = ConnectionError("refused")
        r = runner.invoke(cli, ["status"])
        assert r.exit_code != 0


# ═══════════════════════════════════════
#  Source 子命令
# ═══════════════════════════════════════

class TestSourceCommands:

    @patch("evebus.cli._api_get")
    def test_sources_list(self, mock_get, runner):
        r = runner.invoke(cli, ["sources", "list"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_post")
    def test_sources_add_timer(self, mock_post, runner):
        r = runner.invoke(cli, ["sources", "add-timer", "my_timer", "--topic", "tick", "-i", "2000"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_post")
    def test_sources_add_webhook(self, mock_post, runner):
        r = runner.invoke(cli, ["sources", "add-webhook", "wh1", "--prefix", "ext"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_post")
    def test_sources_start(self, mock_post, runner):
        r = runner.invoke(cli, ["sources", "start", "t1"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_post")
    def test_sources_stop(self, mock_post, runner):
        r = runner.invoke(cli, ["sources", "stop", "t1"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_delete")
    def test_sources_remove(self, mock_delete, runner):
        r = runner.invoke(cli, ["sources", "remove", "t1"])
        assert r.exit_code == 0


# ═══════════════════════════════════════
#  Executor 子命令
# ═══════════════════════════════════════

class TestExecutorCommands:

    @patch("evebus.cli._api_get")
    def test_executors_list(self, mock_get, runner):
        r = runner.invoke(cli, ["executors", "list"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_post")
    def test_executors_add(self, mock_post, runner):
        path = os.path.join(tempfile.gettempdir(), "test_cli_ex.py")
        with open(path, "w") as f:
            f.write("async def on_event(t, e): pass\n")
        r = runner.invoke(cli, ["executors", "add", "ex1", "-s", path, "-t", "test.*"])
        assert r.exit_code == 0
        os.remove(path)

    def test_executors_add_missing_script(self, runner):
        r = runner.invoke(cli, ["executors", "add", "ex1", "-s", "/nonexistent"])
        assert r.exit_code != 0

    @patch("evebus.cli._api_post")
    def test_executors_reload(self, mock_post, runner):
        r = runner.invoke(cli, ["executors", "reload", "ex1"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_delete")
    def test_executors_remove(self, mock_delete, runner):
        r = runner.invoke(cli, ["executors", "remove", "ex1"])
        assert r.exit_code == 0


# ═══════════════════════════════════════
#  Plugin 子命令
# ═══════════════════════════════════════

class TestPluginCommands:

    @patch("evebus.cli._api_get")
    def test_plugins_list(self, mock_get, runner):
        r = runner.invoke(cli, ["plugins", "list"])
        assert r.exit_code == 0

    @patch("evebus.cli._api_delete")
    def test_plugins_remove(self, mock_delete, runner):
        r = runner.invoke(cli, ["plugins", "remove", "p1"])
        assert r.exit_code == 0


# ═══════════════════════════════════════
#  辅助函数
# ═══════════════════════════════════════

class TestHelpers:

    @patch("urllib.request.urlopen")
    def test_api_get_success(self, mock_urlopen):
        resp = MagicMock()
        resp.read.return_value = json.dumps({"ok": True}).encode()
        mock_urlopen.return_value = resp
        _api_get("http://localhost:8080", "/test", "Test")

    @patch("urllib.request.urlopen")
    def test_api_get_error(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("refused")
        _api_get("http://localhost:8080", "/test", "Test")

    @patch("urllib.request.urlopen")
    def test_api_post_success(self, mock_urlopen):
        resp = MagicMock()
        resp.read.return_value = json.dumps({"ok": True}).encode()
        mock_urlopen.return_value = resp
        _api_post("http://localhost:8080", "/test", {"key": "value"})

    @patch("urllib.request.urlopen")
    def test_api_post_error(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("refused")
        _api_post("http://localhost:8080", "/test", {})

    @patch("urllib.request.urlopen")
    def test_api_delete_success(self, mock_urlopen):
        resp = MagicMock()
        resp.read.return_value = json.dumps({"ok": True}).encode()
        mock_urlopen.return_value = resp
        _api_delete("http://localhost:8080", "/test")

    @patch("urllib.request.urlopen")
    def test_api_delete_error(self, mock_urlopen):
        mock_urlopen.side_effect = ConnectionError("refused")
        _api_delete("http://localhost:8080", "/test")