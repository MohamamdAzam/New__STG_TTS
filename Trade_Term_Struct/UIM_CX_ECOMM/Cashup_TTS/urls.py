from django.urls import path
from .views import mrp_upload_file

urlpatterns = [
    path('mrp/upload/', mrp_upload_file, name='mrp_upload_file'),
]