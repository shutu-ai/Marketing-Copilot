import sys
import os
import threading
import logging

import duckdb

sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from database import (
    init_db,
    is_database_read_error,
    is_transient_write_error,
    read_transaction,
    start_stall_watchdog,
    stop_stall_watchdog,
)
from auth import auth_middleware
from services import update_service
from routers import smpp, sms_content, monitor, rabbitmq as rabbitmq_router, db_config as db_config_router, batch_campaign, settings as settings_router, dashboard as dashboard_router, realtime_campaign as realtime_campaign_router, db_query as db_query_router, sms_tasks as sms_tasks_router, operations as operations_router, metrics as metrics_router, filesystem as filesystem_router, logs as logs_router, auth as auth_router, update as update_router, users as users_router, internal as internal_router

from services.runtime_service import configure_logging, acquire_instance_lock, release_instance_lock
configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="Marketing Copilot API", version=update_service.version_info().get("version", "1.2.0"))


def _smpp_unavailable_status() -> dict:
    """Keep health/capacity responses usable while the DB is temporarily down."""
    return {
        "state": "unknown",
        "connected": False,
        "config_id": None,
        "active_config_id": None,
        "enabled_configs": None,
        "pools": [],
        "last_error": "DATABASE_READ_UNAVAILABLE",
    }


def _database_busy_response(request: Request, exc: BaseException) -> JSONResponse:
    logger.warning(
        "Database write contention request=%s method=%s error=%s",
        request.url.path, request.method, exc,
    )
    return JSONResponse(
        status_code=503,
        content={
            "detail": "当前数据库写入繁忙，请稍后重试",
            "code": "DATABASE_WRITE_BUSY",
            "retryable": True,
        },
        headers={"Retry-After": "1"},
    )


@app.exception_handler(TimeoutError)
async def handle_timeout(request: Request, exc: TimeoutError):
    if is_transient_write_error(exc):
        return _database_busy_response(request, exc)
    logger.warning("Request timed out path=%s error=%s", request.url.path, exc)
    return JSONResponse(
        status_code=504,
        content={
            "detail": "请求处理超时，请稍后重试",
            "code": "REQUEST_TIMEOUT",
            "retryable": True,
        },
        headers={"Retry-After": "1"},
    )


@app.exception_handler(duckdb.TransactionException)
async def handle_transaction_conflict(request: Request, exc: duckdb.TransactionException):
    if is_transient_write_error(exc):
        return _database_busy_response(request, exc)
    logger.exception("Unhandled DuckDB transaction error path=%s", request.url.path)
    return JSONResponse(
        status_code=503,
        content={
            "detail": "数据库事务暂时不可用，请稍后重试",
            "code": "DATABASE_TRANSACTION_UNAVAILABLE",
            "retryable": True,
        },
        headers={"Retry-After": "1"},
    )


@app.exception_handler(duckdb.IOException)
async def handle_database_io_error(request: Request, exc: duckdb.IOException):
    logger.warning(
        "Database read I/O unavailable request=%s method=%s error=%s",
        request.url.path, request.method, exc,
    )
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database temporarily unavailable; please retry.",
            "code": "DATABASE_READ_UNAVAILABLE",
            "retryable": True,
        },
        headers={"Retry-After": "1"},
    )


_DUCKDB_ERROR = getattr(duckdb, "Error", None)
if _DUCKDB_ERROR is not None:
    @app.exception_handler(_DUCKDB_ERROR)
    async def handle_database_error(request: Request, exc):
        logger.exception("Unhandled DuckDB error path=%s", request.url.path)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Database temporarily unavailable; please retry.",
                "code": "DATABASE_UNAVAILABLE",
                "retryable": True,
            },
            headers={"Retry-After": "1"},
        )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.middleware("http")(auth_middleware)

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
app.include_router(auth_router.router)
app.include_router(update_router.router)
app.include_router(users_router.router)
app.include_router(internal_router.router)


def _start_background_services():
    """Start services in an order that leaves startup recovery uncontended.

    ``start_scheduler`` performs the one-time ingestion lease recovery before
    it starts its own workers.  Keep it ahead of the SMS result buffer and
    task workers, both of which may begin writing as soon as they start.
    """
    from services.activity_scheduler import start_scheduler
    from services.sms_write_buffer import start as start_sms_write_buffer
    from services.task_worker import start_worker

    start_scheduler()
    start_sms_write_buffer()
    start_worker()


@app.on_event("startup")
def startup():
    from services import bounded_dispatch_service
    bounded_dispatch_service.freeze_mode()
    acquire_instance_lock()
    from services import db_config as db_config_service
    from database import get_database_file, set_database_file
    set_database_file(db_config_service.get_effective_db_path(get_database_file()))
    init_db()
    start_stall_watchdog(
        os.environ.get("MC_STALL_DIAGNOSTICS_DIR"),
        os.environ.get("MC_STALL_DIAGNOSTICS_THRESHOLD_SECONDS", "30"),
    )
    from services import user_service
    user_service.bootstrap_admin()
    update_service.record_pending_result()
    from services.settings_service import get_setting
    from services.runtime_service import set_log_level, set_detail_logging
    try:
        set_log_level(get_setting("log_level", "INFO") or "INFO")
    except ValueError as exc:
        logger.warning("Ignoring invalid persisted log level: %s", exc)
    set_detail_logging(get_setting("detail_logging", "0") or "0")
    os.makedirs(os.path.join(os.path.dirname(__file__), "uploads"), exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(__file__), "monitor_dir"), exist_ok=True)
    update_service.ensure_update_dir()
    _start_background_services()

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
    from services.sms_write_buffer import stop as stop_sms_write_buffer
    # Stop producers first, then disconnect SMPP/stop claims, and only then
    # drain the durable result buffer.  A failure in one stage must not skip
    # the remaining cleanup or leave the instance lock behind.
    for name, stop in (
        ("activity scheduler", stop_scheduler),
        ("SMS workers", stop_worker),
        ("SMS result buffer", stop_sms_write_buffer),
    ):
        try:
            stop()
        except Exception:
            logger.exception("Shutdown stage failed: %s", name)
    try:
        release_instance_lock()
    except Exception:
        logger.exception("Failed to release instance lock during shutdown")
    try:
        stop_stall_watchdog()
    except Exception:
        logger.exception("Failed to stop database stall watchdog during shutdown")


@app.get("/api/health")
def health():
    db_ok = False
    try:
        read_transaction(
            lambda conn: conn.execute("SELECT 1"),
            operation="health",
        )
        db_ok = True
    except Exception:
        pass
    from services.smpp_gateway import gateway
    try:
        smpp = gateway.status()
    except Exception as exc:
        if not is_database_read_error(exc):
            raise
        logger.warning("Health SMPP status unavailable: %s", exc)
        smpp = _smpp_unavailable_status()
    return {"status": "ok" if db_ok else "degraded", "service": "Marketing Copilot",
            "database": "connected" if db_ok else "disconnected", "smpp": smpp}


@app.get("/api/ready")
def ready():
    try:
        read_transaction(
            lambda conn: conn.execute("SELECT 1"),
            operation="ready",
        )
    except Exception as exc:
        if not is_database_read_error(exc):
            raise
        raise HTTPException(
            status_code=503,
            detail="database temporarily unavailable; please retry",
            headers={"Retry-After": "1"},
        ) from exc
    return {"status": "ready", "version": update_service.version_info().get("version", "1.2.0")}


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
