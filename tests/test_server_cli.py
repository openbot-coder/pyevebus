"""Server CLI 测试 — evebus 服务端"""
import os
import tempfile
import pytest
import subprocess
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from evebus.server_cli import server_cli, main


@pytest.fixture
def runner():
    return CliRunner()


# ═══════════════════════════════════════
#  基础命令
# ═══════════════════════════════════════

class TestServerBasic:

    def test_help(self, runner):
        r = runner.invoke(server_cli, ["--help"])
        assert r.exit_code == 0
        assert "EveBus" in r.output
        assert "serve" in r.output
        assert "run" in r.output

    def test_version(self, runner):
        r = runner.invoke(server_cli, ["--version"])
        assert r.exit_code == 0
        assert "evebus" in r.output.lower()

    def test_serve_help(self, runner):
        r = runner.invoke(server_cli, ["serve", "--help"])
        assert r.exit_code == 0
        assert "host" in r.output

    def test_run_help(self, runner):
        r = runner.invoke(server_cli, ["run", "--help"])
        assert r.exit_code == 0
        assert "SCRIPT" in r.output


# ═══════════════════════════════════════
#  serve 命令
# ═══════════════════════════════════════

class TestServeCommand:

    @patch("subprocess.Popen")
    def test_serve_starts_uvicorn(self, mock_popen, runner):
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]

        r = runner.invoke(server_cli, ["serve", "--port", "9999"])
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        assert "uvicorn" in args
        assert any("--port=9999" in a for a in args)

    @patch("subprocess.Popen")
    def test_serve_reload_flag(self, mock_popen, runner):
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]

        r = runner.invoke(server_cli, ["serve", "--reload"])
        args = mock_popen.call_args[0][0]
        assert "--reload" in args

    @patch("subprocess.Popen")
    def test_serve_workers_flag(self, mock_popen, runner):
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]

        r = runner.invoke(server_cli, ["serve", "--workers", "4"])
        args = mock_popen.call_args[0][0]
        assert any("--workers=4" in a for a in args)

    @patch("subprocess.Popen")
    def test_serve_keyboard_interrupt_terminates(self, mock_popen, runner):
        """Ctrl+C → terminate → 正常退出"""
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        # 第一次 wait 抛 KeyboardInterrupt，finally 中的 wait 正常返回
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]

        r = runner.invoke(server_cli, ["serve"])
        assert mock_proc.terminate.called
        assert r.exit_code == 0

    @patch("subprocess.Popen")
    def test_serve_normal_exit(self, mock_popen, runner):
        """子进程正常退出"""
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        mock_proc.wait.return_value = None

        r = runner.invoke(server_cli, ["serve"])
        assert r.exit_code == 0
        assert mock_proc.terminate.called  # finally 块总会调用

    @patch("subprocess.Popen")
    def test_serve_timeout_kills(self, mock_popen, runner):
        """terminate 后超时 → kill"""
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        # 第一次 wait (main) 不抛异常，第二次 wait (finally) 超时，第三次 wait (kill) 正常
        mock_proc.wait.side_effect = [
            None,  # 主流程 process.wait()
            subprocess.TimeoutExpired("cmd", 5),  # finally: process.wait(timeout=5)
            None,  # finally: process.wait() after kill
        ]

        r = runner.invoke(server_cli, ["serve"])
        assert mock_proc.terminate.called
        assert mock_proc.kill.called


# ═══════════════════════════════════════
#  run 命令
# ═══════════════════════════════════════

class TestRunCommand:

    def test_run_missing_script(self, runner):
        r = runner.invoke(server_cli, ["run", "/nonexistent/script.py"])
        assert r.exit_code == 1
        assert "不存在" in r.output

    def test_run_async_flow(self):
        """mock asyncio.run 捕获 _run 协程"""
        captured = {}

        def fake_asyncio_run(coro, **kw):
            captured["coro"] = coro
            try:
                coro.send(None)
            except StopIteration:
                pass
            return None

        path = os.path.join(tempfile.gettempdir(), "srv_run.py")
        with open(path, "w") as f:
            f.write("async def on_event(t, e): pass\n")

        runner = CliRunner()
        with patch("asyncio.run", side_effect=fake_asyncio_run):
            r = runner.invoke(server_cli, ["run", path, "-t", "data.*"])
        assert captured.get("coro") is not None
        os.remove(path)


# ═══════════════════════════════════════
#  main 入口
# ═══════════════════════════════════════

class TestMain:

    def test_main_calls_server_cli(self):
        with patch("evebus.server_cli.server_cli") as mock_cli:
            main()
            mock_cli.assert_called_once()