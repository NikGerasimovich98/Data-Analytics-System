insert into idm_tiktok_payment.tiktok_payment_full 
select * from (
with tiktokers as (
select 
	-- Так как поле author идёт как имя фамилия а в таблице  bo_tiktok_blogger может быть ещё и отчество. Для удобного join по имени 
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
where bt.sheets_name = 'ФЕВРАЛЬ 25'
)
,
tiktokers_stats as (
select 
	tik.first_name,  
    tik.last_name,
	tik.release_date,
	-- Блок нужен т.к в источнике тип поля не INT и могут быть не цифровые значения которые лишние
    COALESCE(CAST(NULLIF(tik.tiktok_reach, '') AS INTEGER), 0) AS tiktok_reach,
    COALESCE(CAST(NULLIF(tik.youtube_reach, '') AS INTEGER), 0) AS youtube_reach,
    COALESCE(CAST(NULLIF(tik.instagram_reach, '') AS INTEGER), 0) AS instagram_reach,
    COALESCE(CAST(NULLIF(tik.likee_reach, '') AS INTEGER), 0) AS likee_reach,
	tik.tiktok_video_link,
    tik.likee_video_link,
    tik.account_name,
	tik.sheets_name,
	tk.region,
	tk.currency,
	tk.grade,
	tk.is_state,
	tk.contract
from tiktokers tik
inner join stg_tiktok.bo_tiktok_blogger tk on 
tik.first_name = substring(tk.blogger FROM '^\S+\s+(\S+)') and tik.last_name = substring(tk.blogger FROM '^\S+')
) 
,
premium_mapping AS (
    SELECT 
        grade,
        start_reach ,
        currency,
        end_reach,
        tiktok,
        instagram,
        youtube,
        likee
    FROM stg_mapping.tiktokers_kpi 
)
,
tiktokers_grade AS (
    SELECT 
        grade,
        currency,
        start_count,
        end_count,
        price
    FROM stg_mapping.tiktokers_grade
)
,
tiktokers_premium as (
SELECT  
    ts.first_name,
    ts.last_name,
    ts.grade,
    ts.currency, 
    pm.currency as crn1,
    pm2.currency as crn2,
    ts.release_date,
    ts.tiktok_video_link,
    ts.tiktok_reach,
    ts.likee_video_link,
    ts.likee_reach,
    -- Так как может быть не цифровое значение поля либо появиться Null. Условие - только int
    coalesce(pm.tiktok, 0) AS tiktok_premium,
    coalesce(pm2.likee, 0) AS likee_premium,
    ts.sheets_name,
    ts.account_name
FROM tiktokers_stats ts
left JOIN premium_mapping pm
    ON ts.tiktok_reach BETWEEN pm.start_reach AND pm.end_reach AND ts.grade = pm.grade and ts.currency = pm.currency 
left JOIN premium_mapping pm2
    ON ts.likee_reach BETWEEN pm2.start_reach AND pm2.end_reach AND ts.grade = pm2.grade  and ts.currency = pm2.currency 
)	
,
video_count AS (
    SELECT
        first_name,
        last_name,
        grade,
        currency,
        sheets_name,
        account_name,
        -- Может быть такое что на источнике нет ссылок на видео но остальная информацию проставлена поэтому поля попадают в выборку. 
        -- Подсчёт только имеющихся ссылок.
        COUNT(case when tiktok_video_link is not null and tiktok_video_link != '' then 1 end ) AS video_count,
        SUM(instagram_reach) as instagram_count,
        SUM(youtube_reach) as youtube_count
    FROM tiktokers_stats 
    GROUP BY 
        first_name,
        last_name,
        grade,
        currency,
        sheets_name,
        account_name
)  
,
video_price as (
	select
        v.first_name,
        v.last_name,
        v.grade,
        v.currency,
        v.video_count,
        v.instagram_count,
        v.youtube_count,
        t.price,
        -- Так как может быть не цифровое значение поля либо появиться Null. Условие - только int
        coalesce(pm.instagram, 0) AS instagram_premium,
    	coalesce(pm2.youtube, 0) AS youtube_premium,
    	v.sheets_name,
    	v.account_name
	from video_count v
	left join tiktokers_grade t 
		on v.grade = t.grade and v.video_count BETWEEN t.start_count AND t.end_count  and v.currency = t.currency 
	left JOIN premium_mapping pm
    	ON v.instagram_count BETWEEN pm.start_reach AND pm.end_reach AND v.grade = pm.grade  and v.currency = pm.currency 
	left JOIN premium_mapping pm2 
	    ON v.youtube_count BETWEEN pm2.start_reach AND pm2.end_reach AND v.grade = pm2.grade  and v.currency = pm2.currency 
)  
, 
final_full as (
select 
    tp.first_name,
    tp.last_name,
    CONCAT(tp.first_name,'  ',tp.last_name) as blogger_name, 
    tp.grade,
    tp.release_date,
    tp.currency,
    vp.video_count,
    vp.price as price_per_video,
    tp.tiktok_video_link,
    tp.tiktok_reach,
    tp.likee_video_link,
    tp.likee_reach,
    vp.instagram_count,
    vp.youtube_count,
    tp.tiktok_premium,
    tp.likee_premium,
    vp.instagram_premium ,
    vp.youtube_premium,
    tp.sheets_name,
    vp.account_name
from tiktokers_premium tp
join video_price vp on tp.first_name = vp.first_name and tp.last_name = vp.last_name
)
,
final_short as (
select 
    fn.first_name,
    fn.last_name,
    CONCAT(fn.first_name,' ',fn.last_name) as blogger_name, 
    fn.grade,
	fn.currency,
	fn.video_count,
	fn.instagram_count,
	fn.youtube_count,
    sum(fn.price_per_video) as price_count_videos,
    sum(fn.tiktok_premium) as tiktok_premium,
    sum(fn.likee_premium) as likee_premium,
    max(fn.instagram_premium) as instagram_premium,
    max(fn.youtube_premium) as youtube_premium,
    fn.sheets_name,
    fn.account_name
from final_full fn
group by 
	fn.first_name,
    fn.last_name,
    fn.grade,
    fn.currency,
    fn.video_count,
	fn.instagram_count,
    fn.youtube_count,
    fn.sheets_name,
    fn.account_name
)
 select distinct
    blogger_name, 
    replace (account_name, 'Отчет','') as account_name,
    grade,
    release_date,
    currency,
    video_count,
    price_per_video,
    tiktok_video_link,
    tiktok_reach,
    likee_video_link,
    likee_reach,
    instagram_count,
    youtube_count,
    tiktok_premium,
    likee_premium,
    instagram_premium ,
    youtube_premium,
    sheets_name
 from final_full
 where tiktok_video_link != '' 
)
