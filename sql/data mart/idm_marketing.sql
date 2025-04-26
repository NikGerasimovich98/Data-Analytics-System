with clean_budget as (
select
	id,
	social_network,
	social_link,
	responsible_person,
	"name",
	price,
	integration_date,
	payment_method,
	planned_payment_date,
	actual_payment_date,
	product_cost,
	video_creation_date,
	actual_reach,
	actual_cpm,
	is_integration,
	region,
	"format",
	sheets_name
FROM stg_marketing.bo_marketing
where sheets_name = 'Апрель 2025' 
	  and id < (
	  			select MIN(id) from stg_marketing.bo_marketing where social_link = 'БЮДЖЕТ ' and sheets_name = 'Апрель 2025' 
	  			)
	  and id not in (
				select id from stg_marketing.bo_marketing where reach = 'Итого Безнал' and sheets_name = 'Апрель 2025' 
	  			)
	  and actual_cpm != '#DIV/0!'			
order by id	  			
) 
,
separator as 
(
select
	id,
    responsible_person AS product_name,
    LEAD(id) OVER (ORDER BY id) AS next_separator_id
from clean_budget where responsible_person like '%-%'
)
,
half_clear as (
SELECT 
	t.id,
    t.social_network,
    t.social_link,
    t.responsible_person,
    t.name,
    REGEXP_REPLACE(t.price,'[\xA0\s]+', '', 'g') as price,
    t.integration_date,
    t.payment_method,
    t.planned_payment_date,
    t.actual_payment_date,
    REGEXP_REPLACE(t.product_cost,'[\xA0\s]+', '', 'g') as product_cost,
    t.video_creation_date,
    REGEXP_REPLACE(t.actual_reach,'[\xA0\s]+', '', 'g') as actual_reach,
    REGEXP_REPLACE(t.actual_cpm,'[\xA0\s]+', '', 'g') as actual_cpm,
    t.is_integration,
	t.region,
	t."format",
	t.sheets_name,
    sep.product_name
FROM clean_budget t
LEFT JOIN separator sep ON t.id >= sep.id 
			AND (t.id < sep.next_separator_id OR sep.next_separator_id IS NULL)
) 
,
clear as (
select
	id,
    social_network,
    social_link,
    responsible_person,
    name,
    COALESCE(CAST(NULLIF(REPLACE(price, ',', '.'), '') AS numeric), 0) as price,
    integration_date,
    payment_method,
    planned_payment_date,
    actual_payment_date,
    COALESCE(CAST(NULLIF(REPLACE(product_cost, ',', '.'), '') AS numeric), 0) as product_cost,
    video_creation_date,
    COALESCE(CAST(NULLIF(REPLACE(actual_reach, ',', '.'), '') AS numeric), 0) as actual_reach,
    COALESCE(CAST(NULLIF(REPLACE(actual_cpm, ',', '.'), '') AS numeric), 0) as actual_cpm,
    is_integration,
	region,
	"format",
    case 
    	when split_part(product_name, '-', 2) like '%tsh%' THEN 'Tashe'
    	when split_part(product_name, '-', 2) like '%lmb%' THEN 'Limba'
    	else 'Nothing'
    end as product_name,
    split_part(product_name, '-', 2) AS articul_product ,
    sheets_name
from half_clear   
where responsible_person not like '%-%'    
)
select 
	'Рекламные интеграции' as department,
	id,
	(price + product_cost) as full_price,
	actual_reach,
	is_integration,
	actual_cpm,
	product_name,
	articul_product,
	"format",
	region,
	sheets_name
from clear 
where is_integration = 'TRUE'
   
    


    
    

