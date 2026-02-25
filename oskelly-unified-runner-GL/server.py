#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from orchestrator import PipelineError, run_pipeline


class IndexStarts(BaseModel):
    brand: int = Field(..., ge=0)
    color: int = Field(..., ge=0)
    material: int = Field(..., ge=0)
    category: int = Field(..., ge=0)
    season: int = Field(..., ge=0)


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_file: str = Field(..., description="Absolute path to input .xlsx file")
    email: str = Field(..., description="Client email used by module outputs")
    index_starts: IndexStarts

    output_root: Optional[str] = Field(
        default=None,
        description="Where unified run folders are created. Default: <repo>/unified-output",
    )
    timezone: str = Field(default="Europe/Tallinn")

    openai_base_url: Optional[str] = Field(default=None, description="Optional override for OPENAI_BASE_URL")

    use_llm_brand: bool = Field(default=True)
    use_llm_season: bool = Field(default=False)
    season_llm_model: Optional[str] = Field(default=None)

    require_llm_success: bool = Field(
        default=True,
        description="If true, pipeline stops when LLM is unavailable/fails.",
    )


app = FastAPI(title="Oskelly Unified Mapping API", version="1.0.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/run")
def run_job(req: RunRequest) -> dict:
    try:
        payload = req.model_dump()
        result = run_pipeline(payload, log=lambda msg: print(msg, flush=True))
        return {"status": "ok", "result": result}
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}") from e
