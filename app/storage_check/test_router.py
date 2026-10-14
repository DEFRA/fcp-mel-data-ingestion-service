from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pymongo.errors import PyMongoError

from app.common.mongo import get_db
from app.storage_check.router import router


@pytest.fixture
def setup_check(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "dev")

    collection = AsyncMock()
    collection.find_one.return_value = {
        "purpose": "synthetic development connectivity test",
    }

    database = MagicMock()
    database.get_collection.return_value = collection

    s3_check = MagicMock(return_value=0)
    monkeypatch.setattr(
        "app.storage_check.router.list_s3_sample",
        s3_check,
    )

    test_app = FastAPI()
    test_app.include_router(router)
    test_app.dependency_overrides[get_db] = lambda: database

    with TestClient(test_app) as client:
        yield client, collection, s3_check


def test_success(setup_check):
    client, collection, s3_check = setup_check

    response = client.post("/internal/storage-check")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["s3"]["sample_objects_listed"] == 0

    s3_check.assert_called_once()
    collection.insert_one.assert_awaited_once()

    inserted = collection.insert_one.call_args.args[0]
    collection.find_one.assert_awaited_once_with({"_id": inserted["_id"]})
    collection.delete_one.assert_awaited_once_with({"_id": inserted["_id"]})


@pytest.mark.parametrize("environment", ["prod", "test", ""])
def test_disabled_outside_dev(setup_check, monkeypatch, environment):
    client, collection, s3_check = setup_check
    monkeypatch.setenv("ENVIRONMENT", environment)

    response = client.post("/internal/storage-check")

    assert response.status_code == 404
    s3_check.assert_not_called()
    collection.insert_one.assert_not_awaited()


def test_s3_failure_does_not_write_to_mongo(setup_check):
    client, collection, s3_check = setup_check
    s3_check.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "ListObjectsV2",
    )

    response = client.post("/internal/storage-check")

    assert response.status_code == 503
    collection.insert_one.assert_not_awaited()


def test_mongo_read_failure_attempts_cleanup(setup_check):
    client, collection, _ = setup_check
    collection.find_one.side_effect = PyMongoError("simulated failure")

    response = client.post("/internal/storage-check")

    assert response.status_code == 503
    collection.delete_one.assert_awaited_once()
