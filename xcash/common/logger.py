import logging

import structlog

# 开发/生产环境共享的前置处理器
shared_processors: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
]


class ExcludeStaticFilesRequestFilter:
    """过滤掉 `django.server` 里静态文件请求的访问日志。"""

    static_prefix = "/static/"

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "django.server":
            return True

        request_line = ""
        if record.args:
            request_line = str(record.args[0])
        else:
            request_line = record.getMessage()

        # runserver 的访问日志第一项是完整请求行，例如 `GET /static/app.css HTTP/1.1`。
        parts = request_line.split(" ", 2)
        if len(parts) >= 2 and parts[1].startswith(self.static_prefix):
            return False

        return True


def configure_structlog() -> None:
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
