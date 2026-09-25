import asyncio
import time
from uuid import UUID

import psycopg
from fastapi import APIRouter, HTTPException
from pydantic import ValidationError

from pulseforge.assistant.models import ExplainRequest, Explanation
from pulseforge.assistant.provider import OpenAIProvider, ProviderFailure
from pulseforge.assistant.service import IncidentNotFound, explain_offline
from pulseforge.config import Settings
from pulseforge.telemetry import (
    ASSISTANT_DURATION,
    ASSISTANT_PROVIDER_FAILURES,
    ASSISTANT_REQUESTS,
    ASSISTANT_TOKENS,
    ASSISTANT_VALIDATION_FAILURES,
    observe_histogram,
    record_counter,
)

_slots = asyncio.Semaphore(2)


def create_assistant_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["assistant"])

    @router.post("/incidents/{incident_id}/explanation", response_model=Explanation)
    async def explain(incident_id: UUID, request: ExplainRequest) -> Explanation:
        started = time.perf_counter()
        try:
            async with asyncio.timeout(25):
                async with _slots:
                    evidence = await asyncio.to_thread(explain_offline, settings, incident_id)
                    if request.mode == "offline":
                        result = evidence
                    else:
                        result = await OpenAIProvider(settings).explain(evidence)
                    record_counter(ASSISTANT_REQUESTS, request.mode, "success")
                    if result.input_tokens is not None:
                        record_counter(ASSISTANT_TOKENS, "input", amount=result.input_tokens)
                    if result.output_tokens is not None:
                        record_counter(ASSISTANT_TOKENS, "output", amount=result.output_tokens)
                    return result
        except IncidentNotFound as exc:
            record_counter(ASSISTANT_REQUESTS, request.mode, "not_found")
            raise HTTPException(status_code=404, detail="incident_not_found") from exc
        except TimeoutError as exc:
            record_counter(ASSISTANT_REQUESTS, request.mode, "timeout")
            raise HTTPException(status_code=503, detail="assistant_timeout") from exc
        except ProviderFailure as exc:
            record_counter(ASSISTANT_PROVIDER_FAILURES, exc.reason)
            record_counter(ASSISTANT_REQUESTS, request.mode, "provider_error")
            raise HTTPException(status_code=503, detail=exc.reason) from exc
        except (ValidationError, ValueError) as exc:
            record_counter(ASSISTANT_VALIDATION_FAILURES)
            record_counter(ASSISTANT_REQUESTS, request.mode, "invalid")
            raise HTTPException(status_code=503, detail="assistant_response_invalid") from exc
        except (psycopg.Error, OSError) as exc:
            record_counter(ASSISTANT_REQUESTS, request.mode, "unavailable")
            raise HTTPException(status_code=503, detail="assistant_unavailable") from exc
        finally:
            observe_histogram(ASSISTANT_DURATION, time.perf_counter() - started, request.mode)

    return router
