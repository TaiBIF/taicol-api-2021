from django.apps import AppConfig


class ApiConfig(AppConfig):
    name = 'api'

    def ready(self):
        from api.utils import _load_rank_maps
        _load_rank_maps()
