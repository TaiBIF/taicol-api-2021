from django.shortcuts import render
from django.http import (
    JsonResponse,
    HttpResponseRedirect,
    Http404,
    HttpResponse,
)
from django.core.paginator import Paginator

# https://app.apiary.io/taicolname/editor


def name(request):
    print(request.GET)
    requests = request.GET['name']
    print(requests.split('&'))
    # status 200: 成功
    # status 500: 未知錯誤
    # status 400: 如果出現不在以下列表的參數
    # 學名ID name_id
    # 學名 scientific_name
    # 俗名 common_name
    # 更新日期 updated_at
    # 建立日期 created_at
    # 分類群 taxon_group
    name_id = request.GET.get('name_id', '')
    scientific_name = request.GET.get('scientific_name', '')
    common_name = request.GET.get('common_name', '')
    updated_at = request.GET.get('updated_at', '')
    created_at = request.GET.get('created_at', '')
    taxon_group = request.GET.get('taxon_group', '')
    print(name_id, scientific_name, common_name,
          updated_at, created_at, taxon_group)

    # connect to remote database
    #

    response = {'status'}
    # https://www.django-rest-framework.org/api-guide/exceptions/
    return JsonResponse(response)
