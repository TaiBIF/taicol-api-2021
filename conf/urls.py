"""conf URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include, re_path
from api import views as api_view

# swagger API
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi


schema_view = get_schema_view(
    openapi.Info(
        title="TaiCOL API",
        default_version='v2',
        description="TaiCOL API說明文件",
        #   terms_of_service="https://www.google.com/policies/terms/",
        #   contact=openapi.Contact(email="contact@snippets.local"),
        #   license=openapi.License(name="BSD License"),
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
)


api_patterns = [    
    path('name', api_view.NameView.as_view()),
    path('taxon', api_view.TaxonView.as_view()),
    path('higherTaxa', api_view.HigherTaxaView.as_view()),
    path('references', api_view.ReferencesView.as_view()),
    path('nameMatch', api_view.NameMatchView.as_view()),
    path('namecode', api_view.NamecodeView.as_view()),
    path('taxonVersion', api_view.TaxonVersionView.as_view()),
    path('web/stat/index', api_view.web_index_stat),
    path('web/stat/statistics', api_view.web_stat_stat),
    path('update_check_usage', api_view.update_check_usage),
    path('get_taxon_by_higher', api_view.get_taxon_by_higher),
    path('generate_checklist', api_view.generate_checklist),
]

urlpatterns = [
    path('admin/', admin.site.urls),
    path('update_solr', api_view.update_solr, name='update_solr'),
    path('update/name', api_view.update_name, name='update_name'),
    path('update/reference', api_view.update_reference, name='update_reference'),
    path('v2/', include(api_patterns)),  # 有 /v2/ 的版本
    path('', include(api_patterns)),    # 沒有 /v2/ 的版本
    re_path(r'^swagger(?P<format>\.json|\.yaml)$', schema_view.without_ui(cache_timeout=0), name='schema-json'),
    re_path(r'^swagger/$', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    re_path(r'^redoc/$', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
]
