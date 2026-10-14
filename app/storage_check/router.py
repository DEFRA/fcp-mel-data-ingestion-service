import asyncio
import logging
import os
from datetime import UTC, datetime
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, HTTPException
from pymongo import ReadPreference
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import PyMongoError

from app.common.mongo import get_db

logger = logging.getLogger(__name__)

# Deliberately limited to this development bucket.
DEV_BUCKET = "dev-fcp-mel-ingestion-files-c63f2"


def require_dev() -> None:
    # Fail closed if the environment is missing or is not dev.
    if os.environ.get("ENVIRONMENT") != "dev":
        raise HTTPException(status_code=404, detail="Not found")


router = APIRouter(
    prefix="/internal/storage-check",
    tags=["Development checks"],
    dependencies=[Depends(require_dev)],
)


def list_s3_sample() -> int:
    s3 = boto3.client(
        "s3",
        region_name="eu-west-2",
        config=Config(
            connect_timeout=5,
            read_timeout=10,
            retries={"mode": "standard", "total_max_attempts": 2},
        ),
    )

    try:
        response = s3.list_objects_v2(
            Bucket=DEV_BUCKET,
            MaxKeys=5,
        )
        return len(response.get("Contents", []))
    finally:
        s3.close()


@router.post("")
async def check_storage(database: AsyncDatabase = Depends(get_db)):
    try:
        sample_count = await asyncio.to_thread(list_s3_sample)
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 smoke test failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="S3 check failed; inspect service logs and permissions.",
        ) from None

    test_id = f"smoke-test-{uuid4()}"
    record = {
        "_id": test_id,
        "purpose": "synthetic development connectivity test",
        "created_at": datetime.now(UTC),
    }

    collection = database.get_collection(
        "development_smoke_tests",
        read_preference=ReadPreference.PRIMARY,
    )

    try:
        try:
            await collection.insert_one(record)
            saved = await collection.find_one({"_id": test_id})
            if not saved or saved.get("purpose") != record["purpose"]:
                raise HTTPException(
                    status_code=503,
                    detail="MongoDB read-back verification failed.",
                )
        finally:
            # Only remove this invocation's synthetic record.
            await collection.delete_one({"_id": test_id})
    except PyMongoError as exc:
        logger.error("MongoDB smoke test failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="MongoDB check failed; inspect service logs and permissions.",
        ) from None

    return {
        "status": "ok",
        "s3": {
            "bucket": DEV_BUCKET,
            "sample_objects_listed": sample_count,
        },
        "mongodb": {
            "insert": "ok",
            "read": "ok",
            "cleanup": "ok",
        },
    }
