"""
EveBus Server CLI — 服务端启动工具

用法:
    evebus serve [--port 8080] [--host 0.0.0.0]
    evebus run <script> [--patterns "*"]
"""

import sys
import os
import subprocess
from pathlib import Path

import click

# 确保可以 import evebus
_project_root = str(Path(__file__).parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
_python_dir = str(Path(__file__).parent.parent)
if _python_dir not in sys.path:
    sys.path.insert(0, _python_dir)


@click.group()
@click.version_option(version="0.2.0", prog_name="evebus")
def server_cli():
    """EveBus — 异步事件引擎服务端"""
    pass


# ══════════════════════════════════════
#  serve — 启动 HTTP 服务
# ══════════════════════════════════════

@server_cli.command()
@click.option("--host", default="0.0.0.0", help="监听地址")
@click.option("--port", "-p", default=8080, type=int, help="监听端口")
@click.option("--reload", is_flag=True, help="开发模式（自动重载）")
@click.option("--workers", "-w", default=1, type=int, help="工作进程数")
@click.option("--log-level", default="info", type=click.Choice(["debug", "info", "warning", "error"]))
def serve(host, port, reload, workers, log_level):
    """启动 HTTP API 服务"""
    click.echo(f"🚀 启动 EveBus 服务...")
    click.echo(f"   地址: http://{host}:{port}")
    click.echo(f"   文档: http://{host}:{port}/docs")
    click.echo(f"   日志: {log_level}")
    click.echo()

    # 构建 uvicorn 命令
    cmd = [
        sys.executable, "-m", "uvicorn",
        "evebus.server:app",
        f"--host={host}",
        f"--port={port}",
        f"--log-level={log_level}",
    ]
    if reload:
        cmd.append("--reload")
        cmd.append(f"--reload-dir={_python_dir}")
    if workers > 1:
        cmd.append(f"--workers={workers}")

    # 设置环境变量
    env = os.environ.copy()
    env["PYTHONPATH"] = _python_dir + ":" + env.get("PYTHONPATH", "")

    process = subprocess.Popen(cmd, env=env)
    try:
        process.wait()
    except KeyboardInterrupt:
        pass
    finally:
        click.echo("\n⏹️  正在停止服务...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        click.echo("✅ 服务已停止")


# ══════════════════════════════════════
#  run — 运行脚本执行器
# ══════════════════════════════════════

@server_cli.command()
@click.argument("script")
@click.option("--patterns", "-t", multiple=True, default=["*"], help="订阅的 patterns")
@click.option("--auto-reload", is_flag=True, help="自动重载脚本")
@click.option("--reload-interval", default=30.0, type=float, help="重载检查间隔(秒)")
@click.option("--name", "-n", default=None, help="执行器名称")
def run(script, patterns, auto_reload, reload_interval, name):
    """运行脚本执行器"""
    script_path = os.path.abspath(script)
    if not os.path.exists(script_path):
        click.echo(f"❌ 脚本不存在: {script_path}", err=True)
        sys.exit(1)

    click.echo(f"📝 运行脚本: {script_path}")
    click.echo(f"   Patterns: {list(patterns)}")
    click.echo(f"   自动重载: {'是' if auto_reload else '否'}")
    click.echo()

    import asyncio
    from evebus import EventEngine, ScriptExecutor

    async def _run():
        engine = EventEngine()
        executor = ScriptExecutor(
            name=name or Path(script).stem,
            script_path=script_path,
            patterns=list(patterns),
            auto_reload=auto_reload,
            reload_interval_sec=reload_interval,
        )
        await engine.add_executor(executor)
        click.echo(f"✅ 执行器已启动: {executor.name}")
        click.echo("   按 Ctrl+C 停止")
        click.echo()

        # 保持运行
        try:
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            click.echo("\n⏹️  停止执行器...")
            await engine.remove_executor(executor.name)
            click.echo("✅ 已停止")

    asyncio.run(_run())


def main():
    server_cli()


if __name__ == "__main__":
    main()
