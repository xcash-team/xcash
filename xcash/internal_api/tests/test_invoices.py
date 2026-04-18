from decimal import Decimal

from django.test import SimpleTestCase

from internal_api.serializers.invoices import InternalInvoiceCreateSerializer


class InternalInvoiceDurationValidationTests(SimpleTestCase):
    """内部 API 账单有效期边界测试。"""

    def test_duration_over_thirty_minutes_is_rejected(self):
        serializer = InternalInvoiceCreateSerializer(
            data={
                "out_no": "internal-duration-order",
                "title": "Internal duration",
                "currency": "USD",
                "amount": Decimal("10"),
                "methods": {"ETH": ["eth"]},
                "duration": 31,
            }
        )

        self.assertFalse(serializer.is_valid())
        self.assertIn("duration", serializer.errors)

