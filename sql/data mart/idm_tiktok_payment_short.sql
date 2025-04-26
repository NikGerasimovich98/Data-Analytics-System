insert into idm_tiktok_payment.tiktok_payment_short_ver2 
select * from
(
with cte1 as (
select 
	substring(bt.author FROM '^\w+') AS first_name,  
    substring(bt.author FROM '^\S+\s+(\S+)') AS last_name,
	bt.release_date,
	bt.tiktok_reach,
	bt.tiktok_video_link,
	bt.youtube_reach,
	bt.instagram_reach,
	bt.likee_video_link,
	bt.likee_reach ,
	bt.account_name,
	bt.sheets_name
from stg_tiktok.bo_tiktokers bt 
where bt.sheets_name = 'МАРТ 25' 
)
,
cte2 as (
select 
	c1.first_name,  
    c1.last_name,
	c1.release_date,
    COALESCE(CAST(NULLIF(c1.tiktok_reach, '') AS INTEGER), 0) AS tiktok_reach,
    COALESCE(CAST(NULLIF(c1.youtube_reach, '') AS INTEGER), 0) AS youtube_reach,
    COALESCE(CAST(NULLIF(c1.instagram_reach, '') AS INTEGER), 0) AS instagram_reach,
    COALESCE(CAST(NULLIF(c1.likee_reach, '') AS INTEGER), 0) AS likee_reach,
	c1.tiktok_video_link,
    c1.likee_video_link,
    c1.account_name,
	c1.sheets_name,
	tk.region,
	tk.currency,
	tk.grade,
	tk.is_state,
	tk.contract
from cte1 c1
inner join stg_mapping.bo_tiktokers_name tk on c1.first_name = substring(tk.blogger FROM '^\S+\s+(\S+)') and c1.last_name = substring(tk.blogger FROM '^\S+')
) 
,
premium_mapping AS (
    SELECT 
        grade,
        currency,
        start_reach ,
        end_reach,
        tiktok,
        instagram,
        youtube,
        likee
    FROM stg_mapping.bo_tiktok_grade
)
,
tiktokers_grade AS (
    SELECT 
        grade,
        currency,
        start_count,
        end_count,
        price
    FROM stg_mapping.bo_kpi
)
,
cte3 as (
SELECT  
    c2.first_name,
    c2.last_name,
    c2.grade,
    c2.currency, 
    pm.currency as crn1,
    pm2.currency as crn2,
    c2.release_date,
    c2.tiktok_video_link,
    c2.tiktok_reach,
    c2.likee_video_link,
    c2.likee_reach,
    coalesce(pm.tiktok, 0) AS tiktok_premium,
    coalesce(pm2.likee, 0) AS likee_premium,
    c2.sheets_name,
    c2.is_state,
    c2.contract
FROM cte2 c2
left JOIN premium_mapping pm
    ON c2.tiktok_reach BETWEEN pm.start_reach AND pm.end_reach AND c2.grade = pm.grade and c2.currency = pm.currency 
left JOIN premium_mapping pm2
    ON c2.likee_reach BETWEEN pm2.start_reach AND pm2.end_reach AND c2.grade = pm2.grade  and c2.currency = pm2.currency 
) 	
,
video_count AS (
    SELECT
        first_name,
        last_name,
        grade,
        currency,
        sheets_name,
		COUNT(CASE WHEN tiktok_video_link LIKE 'http%' THEN 1 END) AS real_tiktok_link_count,
		CASE
        WHEN COUNT(CASE WHEN tiktok_video_link LIKE 'http%' THEN 1 END) > 60 THEN
            60 + COUNT(CASE WHEN LOWER(tiktok_video_link) = 'дубай' THEN 1 END)
        ELSE
            COUNT(CASE WHEN tiktok_video_link LIKE 'http%' THEN 1 END)
    END AS payment_tiktok_link_count,
	    COUNT(CASE WHEN LOWER(tiktok_video_link) = 'дубай' THEN 1 END) AS dubai_count,       
        SUM(instagram_reach) as instagram_count,
        SUM(youtube_reach) as youtube_count
    FROM cte2 
    GROUP BY 
        first_name,
        last_name,
        grade,
        currency,
        sheets_name
) 
,
video_price as (
	select
        v.first_name,
        v.last_name,
        v.grade,
        v.currency,
        v.real_tiktok_link_count,
        v.payment_tiktok_link_count,
        v.dubai_count,
        v.instagram_count,
        v.youtube_count,
        t.price,
        coalesce(pm.instagram, 0) AS instagram_premium,
    	coalesce(pm2.youtube, 0) AS youtube_premium,
    	v.sheets_name
	from video_count v
	left join tiktokers_grade t 
		on v.grade = t.grade and v.payment_tiktok_link_count BETWEEN t.start_count AND t.end_count  and v.currency = t.currency 
	left JOIN premium_mapping pm
    	ON v.instagram_count BETWEEN pm.start_reach AND pm.end_reach AND v.grade = pm.grade  and v.currency = pm.currency 
	left JOIN premium_mapping pm2 
	    ON v.youtube_count BETWEEN pm2.start_reach AND pm2.end_reach AND v.grade = pm2.grade  and v.currency = pm2.currency 
)   
, 
final_full as (
select 
    c3.first_name,
    c3.last_name,
    CONCAT(c3.first_name,'  ',c3.last_name) as blogger_name, 
    c3.grade,
    c3.release_date,
    c3.currency,
    vp.real_tiktok_link_count,
    vp.dubai_count,
    vp.payment_tiktok_link_count,
    vp.price as price_per_video,
    c3.tiktok_video_link,
    c3.tiktok_reach,
    c3.likee_video_link,
    c3.likee_reach,
    vp.instagram_count,
    vp.youtube_count,
    c3.tiktok_premium,
    c3.likee_premium,
    vp.instagram_premium ,
    vp.youtube_premium,
    c3.sheets_name,
    c3.is_state,
    c3.contract
from cte3 c3
join video_price vp on c3.first_name = vp.first_name and c3.last_name = vp.last_name
) 
,
final_short as (
select 
    fn.first_name,
    fn.last_name,
    CONCAT(fn.first_name,' ',fn.last_name) as blogger_name, 
    fn.grade,
	fn.currency,
	fn.real_tiktok_link_count,
	fn.payment_tiktok_link_count,
	fn.dubai_count,
	fn.instagram_count,
	fn.youtube_count,
    (fn.price_per_video * fn.payment_tiktok_link_count) as price_count_videos,
    (fn.price_per_video * fn.dubai_count) as price_count_dubai_videos,
    sum(fn.tiktok_premium) as tiktok_premium,
    sum(fn.likee_premium) as likee_premium,
    max(fn.instagram_premium) as instagram_premium,
    max(fn.youtube_premium) as youtube_premium,
    fn.sheets_name,
    fn.is_state,
    fn.contract
from final_full fn
group by 
	fn.first_name,
    fn.last_name,
    fn.grade,
    fn.currency,
    fn.real_tiktok_link_count,
	fn.instagram_count,
    fn.youtube_count,
    fn.sheets_name,
    fn.price_per_video,
    fn.payment_tiktok_link_count,
    fn.dubai_count,
    fn.is_state,
    fn.contract
) 
,
short_half_filtred as 
(
 select 
    blogger_name, 
    is_state,
    contract,
    grade,
    currency,
    real_tiktok_link_count,
	payment_tiktok_link_count,
	dubai_count,
    price_count_videos,
    price_count_dubai_videos,
    (price_count_videos + price_count_dubai_videos) as all_price_video,
    instagram_count,
    youtube_count,
    tiktok_premium,
    likee_premium,
    instagram_premium ,
    youtube_premium,
    sheets_name,
    (price_count_videos + price_count_dubai_videos + tiktok_premium + likee_premium + instagram_premium + youtube_premium) AS sum_count
 from final_short
)
select 
    blogger_name, 
    case 
    	when is_state = 'TRUE' then 'штат'
    	else contract
    end as state,
    grade,
    currency,
    real_tiktok_link_count,
	payment_tiktok_link_count,
	dubai_count,
    price_count_videos,
    price_count_dubai_videos,
    all_price_video,
    instagram_count,
    youtube_count,
    tiktok_premium,
    likee_premium,
    instagram_premium ,
    youtube_premium,
    sheets_name,
    sum_count
from short_half_filtred
)



