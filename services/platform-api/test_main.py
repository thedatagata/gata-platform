import pytest
import yaml
from fastapi.testclient import TestClient
from pathlib import Path
from main import app, TENANTS_YAML

client = TestClient(app)

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
                }
            }
        ]
    }
    with open(d, "w") as f:
        yaml.dump(content, f)
    return d

def test_update_logic_integrity(mock_tenants_file, monkeypatch):
    """Verifies that logic updates preserve the YAML structure and content."""
    monkeypatch.setattr("main.TENANTS_YAML", mock_tenants_file)
    
    # Mock subprocess to avoid real dbt runs during testing
    class MockProcess:
        returncode = 0
    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: MockProcess())

    new_logic = {"conversion_pattern": "purchase_complete"}
    
    response = client.post(
        "/semantic-layer/update",
        params={"tenant_slug": "stark_industries", "platform": "facebook_ads"},
        json=new_logic
    )

    assert response.status_code == 200
    
    # Verify file was written correctly
    with open(mock_tenants_file, "r") as f:
        updated = yaml.safe_load(f)
    
    assert updated["tenants"][0]["sources"]["facebook_ads"]["logic"] == new_logic
    assert updated["tenants"][0]["slug"] == "stark_industries"
