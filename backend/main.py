import sys
import os
import threading
import logging

sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from database import init_db
from routers import smpp, sms_content, monitor, rabbitmq as rabbitmq_router, db_config as db_config_router, batch_campaign, settings as settings_router, dashboard as dashboard_router, realtime_campaign as realtime_campaign_router, db_query as db_query_router, sms_tasks as sms_tasks_router, operations as operations_router, metrics as metrics_router, filesystem as filesystem_router, logs as logs_router

from services.runtime_service import configure_logging, acquire_instance_lock, release_instance_lock
configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="Marketing Copilot API", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(smpp.router)
app.include_router(sms_content.router)
app.include_router(monitor.router)
app.include_router(rabbitmq_router.router)
app.include_router(db_config_router.router)
app.include_router(batch_campaign.router)
app.include_router(settings_router.router)
app.include_router(dashboard_router.router)
app.include_router(realtime_campaign_router.router)
app.include_router(db_query_router.router)
app.include_router(sms_tasks_router.router)
app.include_router(operations_router.router)
app.include_router(metrics_router.router)
app.include_router(filesystem_router.router)
app.include_router(logs_router.router)


@app.on_event("startup")
def startup():
    acquire_instance_lock()
    from services import db_config as db_config_service
    from database import get_database_file, set_database_file
    set_database_file(db_config_service.get_effective_db_path(get_database_file()))
    init_db()
    from services.settings_service import get_setting
    from services.runtime_service import set_log_level, set_detail_logging
    try:
        set_log_level(get_setting("log_level", "INFO") or "INFO")
    except ValueError as exc:
        logger.warning("Ignoring invalid persisted log level: %s", exc)
    set_detail_logging(get_setting("detail_logging", "0") or "0")
    os.makedirs(os.path.join(os.path.dirname(__file__), "uploads"), exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(__file__), "monitor_dir"), exist_ok=True)
    from services.task_worker import start_worker
    from services.activity_scheduler import start_scheduler
    start_worker()
    start_scheduler()

    try:
        from services.rabbitmq_service import get_config, start_consumer
        config = get_config()
        if config and config.get("enabled"):
            from services.sms_sender import process_message
            threading.Timer(2.0, lambda: start_consumer(process_message)).start()
            logger.info("RabbitMQ consumer scheduled")
    except Exception as e:
        logger.warning("Skipping RabbitMQ consumer: %s", e)


@app.on_event("shutdown")
def shutdown():
    from services.task_worker import stop_worker
    from services.activity_scheduler import stop_scheduler
    stop_scheduler()
    stop_worker()
    release_instance_lock()


@app.get("/api/health")
def health():
    db_ok = False
    try:
        from database import get_connection
        conn = get_connection()
        conn.execute("SELECT 1")
        db_ok = True
    except Exception:
        pass
    from services.smpp_gateway import gateway
    return {"status": "ok" if db_ok else "degraded", "service": "Marketing Copilot",
            "database": "connected" if db_ok else "disconnected", "smpp": gateway.status()}


@app.get("/api/ready")
def ready():
    from database import get_connection
    try:
        conn = get_connection(); conn.execute("SELECT 1"); conn.close()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}")
    return {"status": "ready"}


DIST_DIR = os.path.abspath(os.environ.get("MC_FRONTEND_DIR", os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")))
if os.path.isdir(DIST_DIR):
    assets = os.path.join(DIST_DIR, "assets")
    if os.path.isdir(assets):
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa(full_path: str):
        candidate = os.path.abspath(os.path.join(DIST_DIR, full_path))
        if candidate.startswith(DIST_DIR + os.sep) and os.path.isfile(candidate):
            return FileResponse(candidate)
        return FileResponse(os.path.join(DIST_DIR, "index.html"))
