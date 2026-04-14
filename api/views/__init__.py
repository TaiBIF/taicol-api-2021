from api.views.misc import web_stat_stat, web_index_stat, TaxonVersionView, NamecodeView
from api.views.name_match import NameMatchView
from api.views.reference import ReferencesView
from api.views.higher_taxa import HigherTaxaView
from api.views.taxon import TaxonView
from api.views.name import NameView
from api.views.admin import (
    update_check_usage,
    get_taxon_by_higher,
    generate_checklist,
    update_solr,
    update_name,
    update_reference,
)
