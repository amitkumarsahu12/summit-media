WITH preferred_language AS (
SELECT customer_id, group_concat(name,',') over(PARTITION BY customer_id) as Preferred_film_language FROM (
SELECT A.*, ROW_NUMBER() OVER(PARTITION BY customer_id) as rno FROM (
SELECT DISTINCT rent.customer_id, f.language_id,lang.name, COUNT(f.language_id) OVER(PARTITION BY rent.customer_id,f.language_id) AS lang_count
FROM film f
INNER JOIN inventory inv
ON f.film_id = inv.film_id
INNER JOIN rental rent
ON inv.inventory_id = rent.inventory_id
INNER JOIN language lang
ON f.language_id = lang.language_id
)A)WHERE RNO<=5
),

preferred_film AS (
SELECT DISTINCT customer_id, GROUP_CONCAT(preferred_film_year,',') OVER(PARTITION BY customer_id) AS preferred_film_year FROM (
SELECT DISTINCT rent.customer_id, f.film_id, f.title, f.release_year, rent.inventory_id, ROW_NUMBER() OVER(PARTITION BY rent.customer_id ORDER BY f.release_year DESC) RNO,
CASE
    WHEN f.release_year > 2010 THEN 'New'
    WHEN f.release_year > 2000 THEN '00s'
    WHEN f.release_year > 1990 THEN '90s'
    WHEN f.release_year < 1990 THEN 'Old'
    ELSE 'NA'
END AS preferred_film_year
FROM film f
INNER JOIN inventory inv
ON f.film_id = inv.film_id
INNER JOIN rental rent
ON inv.inventory_id = rent.inventory_id
)WHERE RNO<=2),

category_group AS(
SELECT DISTINCT customer_id, GROUP_CONCAT(name, ',') OVER(PARTITION BY customer_id) AS group_concat FROM(
SELECT customer_id, category_id, name, MAX_COUNT_category, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY MAX_COUNT_category DESC) AS RNO
FROM (
SELECT DISTINCT cu.customer_id, fc.category_id, cg.name, COUNT(fc.category_id) OVER(PARTITION BY cu.customer_id,fc.category_id) AS MAX_COUNT_category
from customer cu
LEFT OUTER join inventory inv
on cu.store_id = inv.store_id
LEFT OUTER join film fm
on inv.film_id = fm.film_id
LEFT OUTER join film_category fc
on fm.film_id = fc.film_id
LEFT OUTER join category cg
on fc.category_id = cg.category_id
))WHERE RNO<=5
)

SELECT cu.customer_id,
cu.first_name||' '||cu.last_name AS name,
CASE 
    WHEN cu.email LIKE '%@%.%' THEN cu.email
    ELSE 'NA'
END AS email,
--floor((julianday(current_timestamp) - julianday(create_date))/30) as membership_age,
cast((julianday(current_timestamp) - julianday(create_date))/30 as int) as membership_age,
CASE 
    WHEN p.customer_id IS NULL THEN '0.00'
    ELSE SUM(p.amount)
END AS revenue,
pl.Preferred_film_language AS Preferred_film_language,
cg.group_concat AS Preferred_film_category,
ad.city_id AS city,
pf.preferred_film_year AS preferred_film_year
FROM customer cu
LEFT OUTER JOIN payment p
ON cu.customer_id = p.customer_id
LEFT OUTER JOIN preferred_language pl
ON cu.customer_id = pl.customer_id
LEFT OUTER JOIN category_group cg
ON cu.customer_id = cg.customer_id
LEFT OUTER JOIN address ad
ON cu.address_id = ad.address_id
INNER JOIN preferred_film pf
ON cu.customer_id = pf.customer_id
GROUP BY 1,2,3,4;