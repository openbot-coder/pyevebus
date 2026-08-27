"""HTTP API 测试 — server.py"""
import json
import pytest
from evebus.engine import EventEngine
from evebus.server import app
from evebus.sources import TimerSource, WebhookSource


@pytest.fixture
def client():
    """FastAPI TestClient"""
    from starlette.testclient import TestClient
    return TestClient(app)


@pytest.fixture
def engine():
    return app.router.routes  # 确保 app 已初始化
    return None  # server 的 engine 是全局单例


# ═══════════════════════════════════════
#  健康检查
# ═══════════════════════════════════════

class TestHealth:

    def test_health(self, client):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"

    def test_stats(self, client):
        r = client.get("/api/v1/stats")
        assert r.status_code == 200
        data = r.json()
        assert "sources" in data
        assert "executors" in data
        assert "plugins" in data


# ═══════════════════════════════════════
#  事件发射
# ═══════════════════════════════════════

class TestEmit:

    def test_emit_event(self, client):
        r = client.post("/api/v1/events/emit", json={
            "topic": "test.api",
            "payload": {"msg": "hello"},
        })
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["topic"] == "test.api"

    def test_emit_by_path(self, client):
        r = client.post("/api/v1/events/emit/data.quotes.ETH", json={"price": 3000})
        assert r.status_code == 200
        data = r.json()
        assert data["topic"] == "data.quotes.ETH"

    def test_emit_with_source(self, client):
        r = client.post("/api/v1/events/emit", json={
            "topic": "test.sourced",
            "payload": {},
            "source": "api",
        })
        assert r.status_code == 200


# ═══════════════════════════════════════
#  Source API
# ═══════════════════════════════════════

class TestSourceAPI:

    def test_list_sources_empty(self, client):
        r = client.get("/api/v1/sources")
        assert r.status_code == 200
        data = r.json()
        assert data["sources"] == []

    def test_add_timer_source(self, client):
        r = client.post("/api/v1/sources/timer", json={
            "name": "test_timer",
            "topic": "timer.tick",
            "interval_ms": 5000,
        })
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["source"]["name"] == "test_timer"
        # 清理
        client.delete("/api/v1/sources/test_timer")

    def test_add_webhook_source(self, client):
        r = client.post("/api/v1/sources/webhook", json={
            "name": "test_wh",
            "path": "/wh",
            "topic_prefix": "wh",
        })
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        client.delete("/api/v1/sources/test_wh")

    def test_get_source_not_found(self, client):
        r = client.get("/api/v1/sources/nope")
        assert r.status_code == 404

    def test_start_stop_source(self, client):
        client.post("/api/v1/sources/timer", json={
            "name": "t1", "topic": "tick", "interval_ms": 100,
        })
        r = client.post("/api/v1/sources/t1/start")
        assert r.status_code == 200
        assert r.json()["ok"] is True
        r = client.post("/api/v1/sources/t1/stop")
        assert r.status_code == 200
        client.delete("/api/v1/sources/t1")

    def test_start_nonexistent(self, client):
        r = client.post("/api/v1/sources/nope/start")
        assert r.status_code == 200
        assert r.json()["ok"] is False

    def test_stop_nonexistent(self, client):
        r = client.post("/api/v1/sources/nope/stop")
        assert r.status_code == 200
        assert r.json()["ok"] is False

    def test_remove_source(self, client):
        client.post("/api/v1/sources/timer", json={
            "name": "t1", "topic": "tick", "interval_ms": 100,
        })
        r = client.delete("/api/v1/sources/t1")
        assert r.status_code == 200
        assert r.json()["ok"] is True

    def test_remove_nonexistent(self, client):
        r = client.delete("/api/v1/sources/nope")
        assert r.status_code == 200
        assert r.json()["ok"] is False


# ═══════════════════════════════════════
#  Executor API
# ═══════════════════════════════════════

class TestExecutorAPI:

    def test_list_executors_empty(self, client):
        r = client.get("/api/v1/executors")
        assert r.status_code == 200
        assert r.json()["executors"] == []

    def test_add_script_executor(self, client):
        import tempfile, os
        path = os.path.join(tempfile.gettempdir(), "test_api_ex.py")
        with open(path, "w") as f:
            f.write("async def on_event(topic, payload): pass\n")
        r = client.post("/api/v1/executors/script", json={
            "name": "api_ex",
            "script_path": path,
            "patterns": ["test.*"],
        })
        assert r.status_code == 200
        assert r.json()["ok"] is True
        client.delete("/api/v1/executors/api_ex")
        os.remove(path)

    def test_get_executor_not_found(self, client):
        r = client.get("/api/v1/executors/nope")
        assert r.status_code == 404

    def test_remove_executor(self, client):
        import tempfile, os
        path = os.path.join(tempfile.gettempdir(), "test_rm.py")
        with open(path, "w") as f:
            f.write("async def on_event(topic, payload): pass\n")
        client.post("/api/v1/executors/script", json={
            "name": "rm1", "script_path": path, "patterns": ["*"],
        })
        r = client.delete("/api/v1/executors/rm1")
        assert r.status_code == 200
        assert r.json()["ok"] is True
        os.remove(path)

    def test_remove_nonexistent(self, client):
        r = client.delete("/api/v1/executors/nope")
        assert r.status_code == 200
        assert r.json()["ok"] is False

    def test_reload_executor(self, client):
        import tempfile, os
        path = os.path.join(tempfile.gettempdir(), "test_rl.py")
        with open(path, "w") as f:
            f.write("async def on_event(topic, payload): pass\n")
        client.post("/api/v1/executors/script", json={
            "name": "rl1", "script_path": path, "patterns": ["*"],
        })
        r = client.post("/api/v1/executors/rl1/reload")
        assert r.status_code == 200
        assert r.json()["ok"] is True
        client.delete("/api/v1/executors/rl1")
        os.remove(path)


# ═══════════════════════════════════════
#  Webhook 注入
# ═══════════════════════════════════════

class TestWebhookAPI:

    def test_webhook_ingest(self, client):
        client.post("/api/v1/sources/webhook", json={
            "name": "wh1", "path": "/ingest", "topic_prefix": "wh",
        })
        r = client.post("/api/v1/webhook/wh1", json={"data": 123})
        assert r.status_code == 200
        assert r.json()["ok"] is True
        client.delete("/api/v1/sources/wh1")

    def test_webhook_not_found(self, client):
        r = client.post("/api/v1/webhook/nope", json={})
        assert r.status_code == 404


# ═══════════════════════════════════════
#  Plugin API
# ═══════════════════════════════════════

class TestPluginAPI:

    def test_list_plugins_empty(self, client):
        r = client.get("/api/v1/plugins")
        assert r.status_code == 200
        assert r.json()["plugins"] == []

    def test_remove_nonexistent(self, client):
        r = client.delete("/api/v1/plugins/nope")
        assert r.status_code == 200
        assert r.json()["ok"] is False