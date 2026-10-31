INSERT INTO product (id, name, price)
VALUES (1, 'Чайник', 2490.00),
       (2, 'Кофемолка', 3990.00),
       (3, 'Тостер', 2990.00)
ON CONFLICT (id) DO NOTHING;
