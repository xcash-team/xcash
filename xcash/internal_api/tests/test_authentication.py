import pytest
from django.test import RequestFactory
from internal_api.authentication import InternalServiceUser
from internal_api.authentication import InternalTokenAuthentication
from rest_framework.exceptions import AuthenticationFailed


@pytest.fixture
def auth():
    return InternalTokenAuthentication()


@pytest.fixture
def rf():
    return RequestFactory()


class TestInternalTokenAuthentication:
    def test_valid_token(self, auth, rf, settings):
        settings.INTERNAL_API_TOKEN = "test-token"
        request = rf.get("/", HTTP_AUTHORIZATION="Bearer test-token")
        user, _ = auth.authenticate(request)
        assert isinstance(user, InternalServiceUser)
        assert user.is_authenticated

    def test_invalid_token(self, auth, rf, settings):
        settings.INTERNAL_API_TOKEN = "test-token"
        request = rf.get("/", HTTP_AUTHORIZATION="Bearer wrong-token")
        with pytest.raises(AuthenticationFailed):
            auth.authenticate(request)

    def test_missing_header(self, auth, rf, settings):
        settings.INTERNAL_API_TOKEN = "test-token"
        request = rf.get("/")
        assert auth.authenticate(request) is None

    def test_wrong_scheme(self, auth, rf, settings):
        settings.INTERNAL_API_TOKEN = "test-token"
        request = rf.get("/", HTTP_AUTHORIZATION="Token test-token")
        assert auth.authenticate(request) is None
