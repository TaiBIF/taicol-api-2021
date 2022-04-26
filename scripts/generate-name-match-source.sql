/* create temporary table of higher taxa in wide format */

CREATE TEMPORARY TABLE IF NOT EXISTS table2 AS (SELECT tmp.taxon_id  , 
max(CASE when tmp.taxon_rank = 'Kingdom' then `name` end) as `kingdom` ,
max(CASE when tmp.taxon_rank = 'Phylum' then `name` end )as `phylum` ,
max(CASE when tmp.taxon_rank = 'Class' then `name` end )as `class` ,
max(CASE when tmp.taxon_rank = 'Order' then `name` end) as `order` ,  
 max(CASE when tmp.taxon_rank = 'Family' then `name` end) as `family` ,  
max(CASE when tmp.taxon_rank = 'Genus' then `name` end) as `genus`  
 from (
 SELECT th.child_taxon_id as taxon_id, tn.name, 
       r.display ->> '$."en-us"' as taxon_rank 
FROM api_taxon_hierarchy th 
JOIN api_taxon t ON th.parent_taxon_id = t.taxon_id 
JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id 
JOIN ranks r ON tn.rank_id = r.id
WHERE th.child_taxon_id IN (select taxon_id FROM api_taxon) and th.child_taxon_id != th.parent_taxon_id
) as tmp
 group by tmp.taxon_id);
 
/* merge with other information based on taxon_id */

select concat(tn.name, ' ' , tn.formatted_authors) as original_name, at.accepted_taxon_name_id as accepted_namecode, 
	   tn.id as namecode, r.display ->> '$."en-us"' as taxon_rank, concat_ws(',',at.common_name_c,  at.alternative_name_c) as common_name_c,
	   t2.*
from taxon_names tn
JOIN api_taxon_usages atu ON tn.id = atu.taxon_name_id
JOIN api_taxon at ON atu.taxon_id = at.taxon_id
JOIN ranks r ON tn.rank_id = r.id
JOIN table2 t2 ON at.taxon_id = t2.taxon_id;

/* export to tsv */