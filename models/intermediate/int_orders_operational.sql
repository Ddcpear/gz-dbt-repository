SELECT 
    date_date,
    orders_margin.orders_id,
    revenue,
    margin + ship.shipping_fee - ship.log_cost - ship.ship_cost AS operational_margin,
    margin

FROM {{ ref('int_orders_margin')}} AS orders_margin

JOIN {{ ref('stg_raw__ship')}} AS ship
ON orders_margin.orders_id = ship.orders_id
