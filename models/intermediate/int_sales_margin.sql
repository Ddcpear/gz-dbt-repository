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
    ROUND(quantity * purchase_price,2) AS purchase_cost,
    ROUND(revenue - purchase_price,2) AS margin,

    FROM sub

