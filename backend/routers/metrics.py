from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from services import metrics_service, backpressure_service, ingestion_job_service
from services.smpp_gateway import gateway
from services.task_worker import worker_status

router = APIRouter(tags=['capacity'])


@router.get('/metrics', response_class=PlainTextResponse)
def prometheus_metrics():
    return metrics_service.prometheus_text()


@router.get('/api/metrics/status')
def capacity_status():
    workers = worker_status()
    metrics_service.observe_runtime_state(workers['running'])
    return {'workers': workers, 'smpp': gateway.status(),
            'ingestion': ingestion_job_service.status(),
            'backpressure': backpressure_service.snapshot(),
            'metrics': metrics_service.snapshot()}


@router.post('/api/metrics/alerts/evaluate')
def evaluate_alerts():
    workers = worker_status()
    return {'alerts': metrics_service.evaluate_alerts(workers['running'], gateway.status())}
