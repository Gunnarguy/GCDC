from grandchase_meta_analyzer import explorer


def test_resolve_explorer_port_uses_requested_port_when_free(monkeypatch) -> None:
    monkeypatch.setattr(explorer, "_is_port_available", lambda port: port == 8502)
    monkeypatch.setattr(explorer, "_has_http_listener", lambda port: False)

    port, reused_existing = explorer.resolve_explorer_port(8502)

    assert port == 8502
    assert reused_existing is False


def test_resolve_explorer_port_reuses_existing_listener(monkeypatch) -> None:
    monkeypatch.setattr(explorer, "_is_port_available", lambda port: False)
    monkeypatch.setattr(explorer, "_has_http_listener", lambda port: port == 8502)

    port, reused_existing = explorer.resolve_explorer_port(8502)

    assert port == 8502
    assert reused_existing is True


def test_resolve_explorer_port_falls_forward_to_next_open_port(monkeypatch) -> None:
    monkeypatch.setattr(explorer, "_has_http_listener", lambda port: False)
    monkeypatch.setattr(
        explorer,
        "_is_port_available",
        lambda port: port in {8504, 8507},
    )

    port, reused_existing = explorer.resolve_explorer_port(8502)

    assert port == 8504
    assert reused_existing is False


def test_resolve_preferred_explorer_ports_uses_first_free_port(monkeypatch) -> None:
    monkeypatch.setattr(explorer, "_has_http_listener", lambda port: False)
    monkeypatch.setattr(
        explorer,
        "_is_port_available",
        lambda port: port == 8507,
    )

    port, reused_existing = explorer.resolve_preferred_explorer_ports(
        [8506, 8507, 8508]
    )

    assert port == 8507
    assert reused_existing is False


def test_resolve_preferred_explorer_ports_reuses_existing_listener(monkeypatch) -> None:
    monkeypatch.setattr(explorer, "_is_port_available", lambda port: False)
    monkeypatch.setattr(explorer, "_has_http_listener", lambda port: port == 8506)

    port, reused_existing = explorer.resolve_preferred_explorer_ports(
        [8506, 8507, 8508]
    )

    assert port == 8506
    assert reused_existing is True
