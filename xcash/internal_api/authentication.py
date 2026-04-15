from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed


class InternalServiceUser(AnonymousUser):
    """内网 API 调用方的虚拟用户，不对应数据库记录。"""

    @property
    def is_authenticated(self):
        return True


class InternalTokenAuthentication(BaseAuthentication):
    """基于静态 Token 的内网 API 认证。

    读取 Authorization: Bearer <token> 头，与 settings.INTERNAL_API_TOKEN 比对。
    """

    keyword = "Bearer"

    def authenticate_header(self, request):
        return self.keyword

    def authenticate(self, request):
        auth_header = request.META.get("HTTP_AUTHORIZATION", "")
        if not auth_header.startswith(f"{self.keyword} "):
            return None

        token = auth_header[len(self.keyword) + 1 :]
        if token != settings.INTERNAL_API_TOKEN:
            raise AuthenticationFailed("Invalid internal API token.")

        return (InternalServiceUser(), None)
