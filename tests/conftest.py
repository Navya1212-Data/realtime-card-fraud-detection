import pytest

from spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    """One Spark session shared by all tests."""
    session = get_spark("tests")
    yield session
    session.stop()