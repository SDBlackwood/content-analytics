def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests",
    )
    parser.addoption(
        "--add-events",
        action="store_true",
        default=False,
        help="Add events to kafka",
    )
