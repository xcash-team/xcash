from django.urls import path

from . import views

app_name = "stress"

urlpatterns = [
    path("webhook/", views.stress_webhook_view, name="webhook"),
]
