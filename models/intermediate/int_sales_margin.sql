WITH sub AS (
    
    SELECT 
    sales.sales_id,
    sales.date_date,
    sales.orders_id,
    sales.products_id,
    sales.revenue,
    sales.quantity,
    products.purchase_price,

FROM {{ ref('stg_raw__sales')}} AS sales

JOIN {{ ref('stg_raw__products')}} AS products
ON products.products_id = sales.products_id
)

SELECT 
    date_date,
    orders_id,
    products_id,
    revenue,
    quantity,
    ROUND(quantity * purchase_price,2) AS purchase_cost,
    ROUND(revenue - (quantity * purchase_price),2) AS margin,
    

    FROM sub

