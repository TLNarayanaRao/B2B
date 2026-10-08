"""Local documentation remains accessible without external guides or arbitrary file reads."""
from fastapi.testclient import TestClient
from backend import main


def test_local_documentation_index_and_content():
    client=TestClient(main.app)
    response=client.get("/api/documentation")
    assert response.status_code==200
    guides=response.json()
    assert len(guides)==8
    assert len({guide["slug"] for guide in guides})==len(guides)
    for guide in guides:
        response=client.get("/api/documentation/"+guide["slug"])
        assert response.status_code==200
        document=response.json()
        assert document["title"]==guide["title"]
        assert document["content"].startswith("# Relay")
        assert ".pdf" not in document["content"]


def test_unknown_document_cannot_read_workspace_files():
    client=TestClient(main.app)
    for slug in ("unknown","README.md","relay.db","%2E%2E%2Fbackend%2Fmain.py"):
        assert client.get("/api/documentation/"+slug).status_code==404


def test_catalogue_is_owned_and_has_no_external_source_links():
    report=TestClient(main.app).get("/api/connector-catalog").json()
    assert report["title"]=="Relay connector reference"
    assert "guide" not in report
    for item in report["connectors"]:
        assert item["documentation"] in main.DOCUMENTATION
        assert "source_url" not in item
        assert "pages" not in item
    assert all(item["name"] in {"HTTP / HTTPS","FTP / FTPS","File","Event stream","LDAP","SFTP","Document library","SMB","AS2","Object storage","JMS messaging"} for item in report["connectors"])
