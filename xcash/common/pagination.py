"""通用分页类。

historical：internal_api 的 List 端点此前直接返回裸数组，
导致 xcash-saas 前端无法依赖 {count, next, previous, results} 渲染分页。
此处提供统一的 page/size 分页器，通过 settings.REST_FRAMEWORK 全局启用，
覆盖所有使用 GenericAPIView 的 ViewSet（currencies/chains 显式设
pagination_class = None 排除在外，数据量小且固定无需分页）。
"""

from rest_framework.pagination import PageNumberPagination


class PageNumberSizePagination(PageNumberPagination):
    """基于 page/size 查询参数的标准分页器。

    - page：页码，1-based，DRF 默认语义
    - size：每页条数，客户端可覆盖默认值
    - page_size：默认 20，兼顾列表页信息密度与单次响应大小
    - max_page_size：上限 100，防止客户端传入异常大值拖慢后端
    """

    page_query_param = "page"
    page_size_query_param = "size"
    page_size = 20
    max_page_size = 100
