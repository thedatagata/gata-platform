import pytest
import os
import yaml
from pathlib import Path
from fastapi.testclient import TestClient
from main import app, TENANTS_YAML

client = TestClient(app)


# --- Existing Tests ---

@pytest.fixture
def mock_tenants_file(tmp_path):
    """Creates a temporary tenants.yaml for isolated testing."""
    d = tmp_path / "tenants.yaml"
    content = {
        "tenants": [
            {
                "slug": "stark_industries",
                "business_name": "Stark Industries",
                "sources": {
                    "facebook_ads": {"enabled": True, "logic": {}}
                },
            }
        ]
    }
    with open(d, "w") as f:
        yaml.dump(content, f)
    return d


def test_update_logic_integrity(mock_tenants_file, monkeypatch):
    """Verifies that logic updates preserve the YAML structure and content."""
    monkeypatch.setattr("main.TENANTS_YAML", mock_tenants_file)

    class MockProcess:
        returncode = 0

    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: MockProcess())

    new_logic = {"conversion_pattern": "purchase_complete"}

    response = client.post(
        "/semantic-layer/update",
        params={"tenant_slug": "stark_industries", "platform": "facebook_ads"},
        json=new_logic,
    )

    assert response.status_code == 200

    with open(mock_tenants_file, "r") as f:
        updated = yaml.safe_load(f)

    assert updated["tenants"][0]["sources"]["facebook_ads"]["logic"] == new_logic
    assert updated["tenants"][0]["slug"] == "stark_industries"


# --- Model Discovery Tests ---

def test_get_models_list():
    response = client.get("/semantic-layer/tyrell_corp/models")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 6


def test_get_model_detail():
    response = client.get(
        "/semantic-layer/tyrell_corp/models/fct_tyrell_corp__ad_performance"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "fct_tyrell_corp__ad_performance"
    assert len(data["dimensions"]) == 5
    assert len(data["measures"]) == 4
    assert len(data["calculated_measures"]) == 3
    assert len(data["joins"]) == 1


def test_get_model_not_found():
    response = client.get("/semantic-layer/tyrell_corp/models/nonexistent")
    assert response.status_code == 404


def test_get_config_not_found():
    response = client.get("/semantic-layer/nonexistent_tenant/config")
    assert response.status_code == 404


# --- Query Endpoint Validation Tests ---

def test_query_endpoint_validation_error():
    response = client.post(
        "/semantic-layer/tyrell_corp/query",
        json={
            "model": "fct_tyrell_corp__ad_performance",
            "dimensions": ["nonexistent_field"],
        },
    )
    assert response.status_code == 400


def test_query_endpoint_invalid_operator():
    response = client.post(
        "/semantic-layer/tyrell_corp/query",
        json={
            "model": "fct_tyrell_corp__ad_performance",
            "dimensions": ["source_platform"],
            "filters": [{"field": "spend", "op": "DROP", "value": "1"}],
        },
    )
    assert response.status_code == 422


# --- Query Execution Test (requires DB) ---

_has_sandbox = Path(__file__).parent.parent.parent.joinpath(
    "warehouse", "sandbox.duckdb"
).exists()
_has_motherduck = bool(os.environ.get("MOTHERDUCK_TOKEN"))


@pytest.mark.skipif(
    not (_has_sandbox or _has_motherduck),
    reason="No database available (sandbox.duckdb or MOTHERDUCK_TOKEN)",
)
def test_query_endpoint_executes(monkeypatch):
    if _has_sandbox and not _has_motherduck:
        monkeypatch.setenv("GATA_ENV", "local")

    response = client.post(
        "/semantic-layer/tyrell_corp/query",
        json={
            "model": "fct_tyrell_corp__ad_performance",
            "dimensions": ["source_platform"],
            "measures": ["spend", "clicks"],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "sql" in data
    assert "data" in data
    assert "columns" in data
    assert "row_count" in data
