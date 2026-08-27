"""
EveBus Client CLI — 远程管理工具

通过 HTTP API 管理远程 EveBus 服务。

用法:
    evebusctl status [--url http://localhost:8080]        查看状态
    evebusctl emit <topic> [-d '{}']                      发射事件
    evebusctl sources [--url ...] list / add-timer / ...  管理事件源
    evebusctl executors [--url ...] list / add / ...      管理执行器
    evebusctl plugins [--url ...] list / remove           管理插件

服务端请使用: evebus serve [--port 8080]
"""

import sys
import os
import json
from pathlib import Path

import click

# 确保可以 import evebus
_project_root = str(Path(__file__).parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
_python_dir = str(Path(__file__).parent.parent)
if _python_dir not in sys.path:
    sys.path.insert(0, _python_dir)

DEFAULT_URL = "http://localhost:8080"


@click.group()
@click.version_option(version="0.3.0", prog_name="evebusctl")
def cli():
    """EveBus Control — 异步事件引擎远程管理工具"""
    pass


# ══════════════════════════════════════
#  status — 查看状态
# ══════════════════════════════════════

@cli.command()
@click.option("--url", "-u", default=DEFAULT_URL, help="服务地址")
def status(url):
    """查看引擎状态"""
    import urllib.request

    try:
        req = urllib.request.urlopen(f"{url}/api/v1/health")
        data = json.loads(req.read())
    except Exception as e:
        click.echo(f"❌ 无法连接服务: {url}", err=True)
        click.echo(f"   {e}", err=True)
        sys.exit(1)

    stats = data.get("stats", {})

    click.echo(f"📊 EventEngine 状态")
    click.echo(f"   服务: {data.get('status', 'unknown')}")
    click.echo()

    # Sources
    s = stats.get("sources", {})
    click.echo(f"📡 事件源: {s.get('count', 0)} 个 (运行中: {s.get('running', 0)})")
    for name in s.get("names", []):
        click.echo(f"   - {name}")
    click.echo()

    # Executors
    e = stats.get("executors", {})
    click.echo(f"⚙️  执行器: {e.get('count', 0)} 个")
    for name in e.get("names", []):
        click.echo(f"   - {name}")
    click.echo(f"   总执行: {e.get('total_executed', 0)}")
    click.echo(f"   总错误: {e.get('total_errors', 0)}")
    click.echo()

    # Plugins
    p = stats.get("plugins", {})
    click.echo(f"🔌 插件: {p.get('count', 0)} 个")
    for name in p.get("names", []):
        click.echo(f"   - {name}")
    click.echo()

    # Handlers
    h = stats.get("handlers", {})
    click.echo(f"📋 Handlers: {h.get('count', 0)} 个")
    click.echo(f"   Pending: {stats.get('pending_tasks', 0)}")


# ══════════════════════════════════════
#  emit — 发射事件
# ══════════════════════════════════════

@cli.command()
@click.argument("topic")
@click.option("--data", "-d", default="{}", help="事件数据 (JSON)")
@click.option("--url", "-u", default=DEFAULT_URL, help="服务地址")
def emit(topic, data, url):
    """发射事件到引擎"""
    import urllib.request

    try:
        payload = json.loads(data)
    except json.JSONDecodeError as e:
        click.echo(f"❌ 无效的 JSON: {e}", err=True)
        sys.exit(1)

    req_data = json.dumps({
        "topic": topic,
        "payload": payload,
    }).encode()

    try:
        req = urllib.request.Request(
            f"{url}/api/v1/events/emit",
            data=req_data,
            headers={"Content-Type": "application/json"},
        )
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read())
    except Exception as e:
        click.echo(f"❌ 发送失败: {e}", err=True)
        sys.exit(1)

    if result.get("handled"):
        click.echo(f"✅ 事件已发射: {topic}")
    else:
        click.echo(f"⚠️  事件已发射但无 handler 匹配: {topic}")


# ══════════════════════════════════════
#  subscribe — 流式订阅事件 (SSE)
# ══════════════════════════════════════

@cli.command()
@click.argument("pattern", default="*")
@click.option("--url", "-u", default=DEFAULT_URL, help="服务地址")
@click.option("--no-color", is_flag=True, help="不输出彩色时间戳")
def subscribe(pattern, url, no_color):
    """流式订阅事件（SSE 推送）"""
    import asyncio
    try:
        from evebus.rpc import RPCClient
    except ImportError as e:
        click.echo(f"❌ 缺少依赖: {e}（需要 httpx）", err=True)
        sys.exit(1)

    click.echo(f"🔔 订阅中: {pattern or '*'}")
    click.echo(f"   {url}/api/v1/events/subscribe?pattern={pattern or '*'}")
    click.echo("   按 Ctrl+C 停止")
    click.echo()

    client = RPCClient(url)

    async def _run():
        try:
            async for event in client.subscribe(pattern or "*", auto_reconnect=True):
                ts = event.get("timestamp", 0) // 1_000_000_000  # ns → s
                if no_color:
                    click.echo(json.dumps(event, ensure_ascii=False, default=str))
                else:
                    click.echo(
                        f"\033[90m[{ts}]\033[0m \033[96m{event.get('topic')}\033[0m "
                        f"{json.dumps(event.get('event'), ensure_ascii=False, default=str)}"
                    )
        except KeyboardInterrupt:
            click.echo("\n⏹️  已停止订阅")

    asyncio.run(_run())


# ══════════════════════════════════════
#  sources — 管理事件源
# ══════════════════════════════════════

@cli.group()
@click.option("--url", "-u", default=DEFAULT_URL, help="服务地址", is_eager=True, expose_value=True)
@click.pass_context
def sources(ctx, url):
    """管理事件源"""
    ctx.ensure_object(dict)
    ctx.obj["url"] = url


@sources.command("list")
@click.pass_context
def sources_list(ctx):
    """列出所有事件源"""
    _api_get(ctx.obj["url"], "/api/v1/sources", "📡 事件源")


@sources.command("add-timer")
@click.argument("name")
@click.option("--topic", default="timer.tick", help="事件 topic")
@click.option("--interval", "-i", default=1000, type=int, help="间隔(ms)")
@click.option("--data", "-d", default="{}", help="事件数据(JSON)")
@click.pass_context
def sources_add_timer(ctx, name, topic, interval, data):
    """添加定时器事件源"""
    payload = json.loads(data) if data else {}
    _api_post(ctx.obj["url"], "/api/v1/sources/timer", {
        "name": name,
        "topic": topic,
        "interval_ms": interval,
        "payload": payload,
    })


@sources.command("add-webhook")
@click.argument("name")
@click.option("--path", default="/ingest", help="Webhook 路径")
@click.option("--prefix", default="webhook", help="Topic 前缀")
@click.pass_context
def sources_add_webhook(ctx, name, path, prefix):
    """添加 Webhook 事件源"""
    _api_post(ctx.obj["url"], "/api/v1/sources/webhook", {
        "name": name,
        "path": path,
        "topic_prefix": prefix,
    })


@sources.command("start")
@click.argument("name")
@click.pass_context
def sources_start(ctx, name):
    """启动事件源"""
    _api_post(ctx.obj["url"], f"/api/v1/sources/{name}/start")


@sources.command("stop")
@click.argument("name")
@click.pass_context
def sources_stop(ctx, name):
    """停止事件源"""
    _api_post(ctx.obj["url"], f"/api/v1/sources/{name}/stop")


@sources.command("remove")
@click.argument("name")
@click.pass_context
def sources_remove(ctx, name):
    """移除事件源"""
    _api_delete(ctx.obj["url"], f"/api/v1/sources/{name}")


# ══════════════════════════════════════
#  executors — 管理执行器
# ══════════════════════════════════════

@cli.group()
@click.option("--url", "-u", default=DEFAULT_URL, help="服务地址", is_eager=True, expose_value=True)
@click.pass_context
def executors(ctx, url):
    """管理执行器"""
    ctx.ensure_object(dict)
    ctx.obj["url"] = url


@executors.command("list")
@click.pass_context
def executors_list(ctx):
    """列出所有执行器"""
    _api_get(ctx.obj["url"], "/api/v1/executors", "⚙️  执行器")


@executors.command("add")
@click.argument("name")
@click.option("--script", "-s", required=True, help="脚本路径")
@click.option("--patterns", "-t", multiple=True, default=["*"], help="订阅 patterns")
@click.option("--auto-reload", is_flag=True, help="自动重载")
@click.pass_context
def executors_add(ctx, name, script, patterns, auto_reload):
    """添加脚本执行器"""
    script_path = os.path.abspath(script)
    if not os.path.exists(script_path):
        click.echo(f"❌ 脚本不存在: {script_path}", err=True)
        sys.exit(1)

    _api_post(ctx.obj["url"], "/api/v1/executors/script", {
        "name": name,
        "script_path": script_path,
        "patterns": list(patterns),
        "auto_reload": auto_reload,
    })


@executors.command("reload")
@click.argument("name")
@click.pass_context
def executors_reload(ctx, name):
    """重载执行器脚本"""
    _api_post(ctx.obj["url"], f"/api/v1/executors/{name}/reload")


@executors.command("remove")
@click.argument("name")
@click.pass_context
def executors_remove(ctx, name):
    """移除执行器"""
    _api_delete(ctx.obj["url"], f"/api/v1/executors/{name}")


# ══════════════════════════════════════
#  plugins — 管理插件
# ══════════════════════════════════════

@cli.group()
@click.option("--url", "-u", default=DEFAULT_URL, help="服务地址", is_eager=True, expose_value=True)
@click.pass_context
def plugins(ctx, url):
    """管理插件"""
    ctx.ensure_object(dict)
    ctx.obj["url"] = url


@plugins.command("list")
@click.pass_context
def plugins_list(ctx):
    """列出所有插件"""
    _api_get(ctx.obj["url"], "/api/v1/plugins", "🔌 插件")


@plugins.command("remove")
@click.argument("name")
@click.pass_context
def plugins_remove(ctx, name):
    """移除插件"""
    _api_delete(ctx.obj["url"], f"/api/v1/plugins/{name}")


# ══════════════════════════════════════
#  辅助函数
# ══════════════════════════════════════

def _api_get(url, path, title):
    import urllib.request
    try:
        req = urllib.request.urlopen(f"{url}{path}")
        data = json.loads(req.read())
        click.echo(f"{title}:")
        click.echo(json.dumps(data, indent=2, ensure_ascii=False))
    except Exception as e:
        click.echo(f"❌ 请求失败: {e}", err=True)


def _api_post(url, path, payload=None):
    import urllib.request
    try:
        req_data = json.dumps(payload or {}).encode()
        req = urllib.request.Request(
            f"{url}{path}",
            data=req_data,
            headers={"Content-Type": "application/json"},
        )
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read())
        click.echo(json.dumps(data, indent=2, ensure_ascii=False))
    except Exception as e:
        click.echo(f"❌ 请求失败: {e}", err=True)


def _api_delete(url, path):
    import urllib.request
    try:
        req = urllib.request.Request(f"{url}{path}", method="DELETE")
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read())
        click.echo(json.dumps(data, indent=2, ensure_ascii=False))
    except Exception as e:
        click.echo(f"❌ 请求失败: {e}", err=True)


def main():
    cli()


if __name__ == "__main__":
    main()