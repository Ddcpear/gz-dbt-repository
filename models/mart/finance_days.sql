SELECT
    date_date
   ,COUNT(orders_id) AS nb_transactions
   ,ROUND(SUM(revenue),2) AS revenue
   ,ROUND(SUM(revenue) / COUNT(orders_id),2) AS av_basket
   ,ROUND(SUM(margin),2) AS margin
   ,ROUND(SUM(operational_margin),2) AS operational_margin
   FROM {{ ref('int_orders_operational')}}

   GROUP BY date_date
   ORDER BY date_date DESC