from django.test import TestCase


class TestConditionalRouting(TestCase):
    """验证 /internal/v1/ 路由在测试环境下可用。

    测试环境 settings.INTERNAL_API_TOKEN = "test-internal-token"，
    路由应当已挂载。未认证时返回 401（而非 404）即证明路由存在。
    """

    def test_internal_routes_exist(self):
        """Token 已设置时，/internal/v1/ 路由存在（返回 401 而非 404）。"""
        response = self.client.get("/internal/v1/projects")
        assert response.status_code != 404

    def test_internal_route_requires_auth(self):
        """无 Token 认证头时返回 401。"""
        response = self.client.get("/internal/v1/projects")
        assert response.status_code == 401

    def test_internal_route_with_valid_token(self):
        """携带正确 Token 时返回 200。"""
        response = self.client.get(
            "/internal/v1/projects",
            HTTP_AUTHORIZATION="Bearer test-internal-token",
        )
        assert response.status_code == 200
