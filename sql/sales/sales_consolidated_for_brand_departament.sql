SELECT ID_MARCA
     , ID_LINHA
     , MARCA
     , LINHA
     , SUM(QTD_VENDA) AS TOTAL_VENDA
FROM `grupoboticario-case.raw_sales.sales_base`
GROUP BY 1,2,3,4