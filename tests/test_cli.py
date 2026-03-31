from grandchase_meta_analyzer.cli import build_parser


def test_parser_accepts_explorer_command() -> None:
    parser = build_parser()

    args = parser.parse_args(["explorer", "--port", "8601", "--headless"])

    assert args.command == "explorer"
    assert args.port == 8601
    assert args.headless is True


def test_parser_uses_configured_explorer_port_pool_by_default() -> None:
    parser = build_parser()

    args = parser.parse_args(["explorer"])

    assert args.command == "explorer"
    assert args.port is None


def test_parser_accepts_pages_command() -> None:
    parser = build_parser()

    args = parser.parse_args(["pages"])

    assert args.command == "pages"
