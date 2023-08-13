gb_data_hackers_episodes=[  
     {
         'name': 'id', 
         'type': 'STRING', 
         'description': 'The Spotify ID for the show'
     },
     {
         'name': 'name', 
         'type': 'STRING', 
         'description': 'The name of the episode'
     },
     {
         'name': 'description', 
         'type': 'STRING', 
         'description': 'A description of the episode'
     },
     {
         'name': 'release_date', 
         'type': 'DATE', 
         'description': 'The date the episode was first released, for example "1981-12-15. Depending on the precision, it might be shown as "1981" or "1981-12"'
     },
     {
         'name': 'duration_ms', 
         'type': 'INTEGER', 'description': 
         'The episode length in milliseconds'
     },
     {
         'name': 'languages', 
         'type': 'STRING', 
         'description': 'A list of the languages used in the episode, identified by their ISO 639-1 code'
     },
     {
         'name': 'explicit', 
         'type': 'BOOL', 
         'description': 'Whether or not the episode has explicit content (true = yes it does; false = no it does not OR unknown)'
     },
     {
         'name': 'type', 
         'type': 'STRING', 
         'description': 'The object type'
     },
]

sales_consolidated_for_brand_departament=[
    {
         'name': 'ID_MARCA', 
         'type': 'INT64', 
         'description': 'Identificador único da marca vendedora'        
    },
    {
         'name': 'ID_LINHA', 
         'type': 'INT64', 
         'description': 'Identificador único da linha/departamento a qual pertence os produtos vendidos'        
    },   
    {
         'name': 'MARCA', 
         'type': 'STRING', 
         'description': 'Nome da marca vendedora'        
    },
    {
         'name': 'LINHA', 
         'type': 'STRING', 
         'description': 'Nome da linha/departamento a qual pertence os produtos vendidos'        
    }, 
    {
         'name': 'TOTAL_VENDA', 
         'type': 'INT64', 
         'description': 'Somatório de todas as vendas por marca e linha'        
    },           
]

sales_consolidated_for_brand_year_month=[
    {
         'name': 'ID_MARCA', 
         'type': 'INT64', 
         'description': 'Identificador único da marca vendedora'        
    },
    {
         'name': 'MARCA', 
         'type': 'STRING', 
         'description': 'Nome da marca vendedora'        
    },
    {
         'name': 'ANO', 
         'type': 'INT64', 
         'description': 'Ano no qual as vendas foram efetuadas pela marca'        
    },
    {
         'name': 'MES', 
         'type': 'INT64', 
         'description': 'Mês no qual as vendas foram efetuadas pela marca'        
    },
    {
         'name': 'TOTAL_VENDA', 
         'type': 'INT64', 
         'description': 'Somatório de todas as vendas da marca no mês e ano em questão'        
    },   
]

sales_consolidated_for_departament_year_month=[
    {
         'name': 'ID_LINHA', 
         'type': 'INT64', 
         'description': 'Identificador único da linha/departamento a qual pertence os produtos vendidos'        
    },     
    {
         'name': 'LINHA', 
         'type': 'STRING', 
         'description': 'Nome da linha/departamento a qual pertence os produtos vendidos'        
    }, 
    {
         'name': 'ANO', 
         'type': 'INT64', 
         'description': 'Ano no qual as vendas foram efetuadas pela linha'        
    },
    {
         'name': 'MES', 
         'type': 'INT64', 
         'description': 'Mês no qual as vendas foram efetuadas pela linha'        
    },
    {
         'name': 'TOTAL_VENDA', 
         'type': 'INT64', 
         'description': 'Somatório de todas as vendas da linha no mês e ano em questão'         
    },   
]

sales_consolidated_for_year_month=[

    {
         'name': 'ANO', 
         'type': 'INT64', 
         'description': 'Ano de referência no qual as vendas foram efetuadas'        
    },
    {
         'name': 'MES', 
         'type': 'INT64', 
         'description': 'Mês no qual as vendas foram efetuadas'        
    },
    {
         'name': 'TOTAL_VENDA', 
         'type': 'INT64', 
         'description': 'Somatório de todas as vendas no mês e ano em questão'         
    },   
]

SCHEMAS = {
    'sales_consolidated_for_brand_departament': sales_consolidated_for_brand_departament,
    'sales_consolidated_for_brand_year_month': sales_consolidated_for_brand_year_month,
    'sales_consolidated_for_departament_year_month': sales_consolidated_for_departament_year_month,
    'sales_consolidated_for_year_month': sales_consolidated_for_year_month
}