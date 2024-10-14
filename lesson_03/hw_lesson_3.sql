--Завдання на SQL до лекції 03.

-- 1.Вивести кількість фільмів в кожній категорії. Результат відсортувати за спаданням.
SELECT
	category.name,
	COUNT(film.film_id) AS film_count
FROM film
JOIN film_category ON film.film_id = film_category.film_id
JOIN category ON film_category.category_id = category.category_id
GROUP BY category.name
ORDER BY film_count DESC

-- 2. Вивести 10 акторів, чиї фільми брали на прокат найбільше. Результат відсортувати за спаданням.
SELECT
	actor.first_name,
	actor.last_name,
	COUNT(rental.rental_id) AS rental_count
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
JOIN film ON film_actor.film_id = film.film_id
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
GROUP BY actor.first_name, actor.last_name
ORDER BY rental_count DESC
LIMIT 10

-- 3. Вивести категорія фільмів, на яку було витрачено найбільше грошей в прокаті
SELECT
	category.name,
	SUM(payment.amount) AS total_amount
FROM payment
JOIN rental ON payment.rental_id = rental.rental_id
JOIN inventory ON rental.inventory_id = inventory.inventory_id
JOIN film ON inventory.film_id = film.film_id
JOIN film_category ON film.film_id = film_category.film_id
JOIN category ON film_category.category_id = category.category_id
GROUP BY category.name
ORDER BY total_amount DESC
LIMIT 1

-- 4. Вивести назви фільмів, яких не має в inventory. Запит має бути без оператора IN
SELECT title
FROM film
WHERE film_id NOT IN (SELECT film_id FROM inventory)

-- 5. Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
SELECT
	actor.first_name,
	actor.last_name,
	COUNT(film.film_id) AS film_count
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
JOIN film ON film_actor.film_id = film.film_id
JOIN film_category ON film.film_id = film_category.film_id
JOIN category ON film_category.category_id = category.category_id
WHERE category.name = 'Children'
GROUP BY actor.first_name, actor.last_name
ORDER BY film_count DESC
LIMIT 3

SELECT * FROM 