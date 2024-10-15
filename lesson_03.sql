--Завдання на SQL до лекції 03.

-- 1.Вивести кількість фільмів в кожній категорії. Результат відсортувати за спаданням.
SELECT
	c.name,
	COUNT(f.film_id) AS film_count
FROM film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY film_count DESC

-- 2. Вивести 10 акторів, чиї фільми брали на прокат найбільше. Результат відсортувати за спаданням.
SELECT
	a.first_name,
	a.last_name,
	COUNT(r.rental_id) AS rental_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film f ON fa.film_id = f.film_id
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY a.first_name, a.last_name
ORDER BY rental_count DESC
LIMIT 10

-- 3. Вивести категорія фільмів, на яку було витрачено найбільше грошей в прокаті
SELECT
	c.name,
	SUM(p.amount) AS total_amount
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY total_amount DESC
LIMIT 1

-- 4. Вивести назви фільмів, яких не має в inventory. Запит має бути без оператора IN
SELECT f.title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.film_id IS NULL

-- 5. Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
SELECT
	a.first_name,
	a.last_name,
	COUNT(f.film_id) AS film_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film f ON fa.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
WHERE c.name = 'Children'
GROUP BY a.first_name, a.last_name
ORDER BY film_count DESC
LIMIT 3

