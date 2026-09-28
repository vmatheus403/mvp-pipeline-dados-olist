# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Qualidade de Dados da Camada Bronze
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Avaliar a qualidade dos dados brutos do dataset Olist antes da
# MAGIC construção da camada Silver.
# MAGIC
# MAGIC As verificações realizadas contemplam:
# MAGIC
# MAGIC - volume e estrutura das tabelas;
# MAGIC - completude dos atributos;
# MAGIC - unicidade das chaves;
# MAGIC - integridade referencial;
# MAGIC - consistência de valores categóricos;
# MAGIC - validade de datas e valores numéricos;
# MAGIC - identificação inicial de outliers.
# MAGIC
# MAGIC Nesta etapa, nenhum registro é alterado ou removido. Os resultados
# MAGIC serão utilizados para definir e justificar as transformações da
# MAGIC camada Silver.
# MAGIC
# MAGIC ## Fonte
# MAGIC
# MAGIC Tabelas Delta do schema `workspace.bronze`.
# MAGIC
# MAGIC ## Princípio adotado
# MAGIC
# MAGIC Problemas de qualidade são primeiro medidos e documentados. Somente
# MAGIC depois são definidas as regras de tratamento.

# COMMAND ----------

# ============================================================
# CONFIGURAÇÃO DA ANÁLISE DE QUALIDADE
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOGO = "workspace"
SCHEMA_BRONZE = "bronze"

TABELAS_BRONZE = [
    "category_translation",
    "customers",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers"
]

print("=" * 60)
print("ANÁLISE DE QUALIDADE DA CAMADA BRONZE")
print("=" * 60)
print(f"Origem: {CATALOGO}.{SCHEMA_BRONZE}")
print(f"Tabelas previstas: {len(TABELAS_BRONZE)}")
print("=" * 60)

# COMMAND ----------

# Confirmar a existência das tabelas necessárias

tabelas_existentes = {
    linha.tableName
    for linha in spark.sql(
        "SHOW TABLES IN workspace.bronze"
    ).collect()
}

tabelas_faltantes = (
    set(TABELAS_BRONZE) - tabelas_existentes
)

print(f"Tabelas encontradas: {len(set(TABELAS_BRONZE) & tabelas_existentes)}")

if not tabelas_faltantes:
    print("Todas as tabelas Bronze necessárias foram encontradas: OK")
else:
    print("Tabelas faltantes:")
    for tabela in sorted(tabelas_faltantes):
        print(f"- {tabela}")

# COMMAND ----------

# ============================================================
# INVENTÁRIO DAS TABELAS BRONZE
# ============================================================

inventario = []

for tabela in TABELAS_BRONZE:

    nome_completo = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"
    df = spark.table(nome_completo)

    arquivos_origem = (
        df.select("_source_file")
        .distinct()
        .count()
        if "_source_file" in df.columns
        else 0
    )

    inventario.append({
        "tabela": nome_completo,
        "registros": df.count(),
        "colunas": len(df.columns),
        "arquivos_origem_distintos": arquivos_origem
    })

df_inventario = spark.createDataFrame(inventario)

display(
    df_inventario.orderBy("tabela")
)

# COMMAND ----------

# ============================================================
# ANÁLISE DE COMPLETUDE
# ============================================================

colunas_tecnicas = {
    "_source_file",
    "_ingestion_timestamp",
    "_ingestion_date"
}

resultados_completude = []

for tabela in TABELAS_BRONZE:

    nome_completo = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"
    df = spark.table(nome_completo)

    total_registros = df.count()

    colunas_dados = [
        coluna
        for coluna in df.columns
        if coluna not in colunas_tecnicas
    ]

    expressoes_nulos = []

    for coluna in colunas_dados:
        expressoes_nulos.append(
            F.sum(
                F.when(
                    F.col(coluna).isNull()
                    | (
                        F.trim(
                            F.col(coluna).cast("string")
                        ) == ""
                    ),
                    1
                ).otherwise(0)
            ).alias(coluna)
        )

    contagens = (
        df.agg(*expressoes_nulos)
        .first()
        .asDict()
    )

    for coluna, quantidade_nulos in contagens.items():

        quantidade_nulos = quantidade_nulos or 0

        percentual_nulos = (
            round(
                quantidade_nulos * 100.0 / total_registros,
                4
            )
            if total_registros > 0
            else 0.0
        )

        resultados_completude.append({
            "tabela": nome_completo,
            "coluna": coluna,
            "total_registros": total_registros,
            "valores_ausentes": quantidade_nulos,
            "percentual_ausentes": percentual_nulos
        })

df_completude = spark.createDataFrame(
    resultados_completude
)

display(
    df_completude
    .filter(F.col("valores_ausentes") > 0)
    .orderBy(
        F.desc("percentual_ausentes"),
        "tabela",
        "coluna"
    )
)

# COMMAND ----------

# ============================================================
# CHAVES ESPERADAS POR TABELA
# ============================================================

CHAVES_ESPERADAS = {
    "category_translation": [
        "product_category_name"
    ],
    "customers": [
        "customer_id"
    ],
    "order_items": [
        "order_id",
        "order_item_id"
    ],
    "order_payments": [
        "order_id",
        "payment_sequential"
    ],
    "order_reviews": [
        "review_id"
    ],
    "orders": [
        "order_id"
    ],
    "products": [
        "product_id"
    ],
    "sellers": [
        "seller_id"
    ]
}

print(f"Chaves configuradas para {len(CHAVES_ESPERADAS)} tabelas.")

# COMMAND ----------

# ============================================================
# ANÁLISE DE UNICIDADE DAS CHAVES
# ============================================================

resultados_unicidade = []

for tabela, chaves in CHAVES_ESPERADAS.items():

    nome_completo = (
        f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"
    )

    df = spark.table(nome_completo)

    total_registros = df.count()

    condicao_chave_nula = None

    for chave in chaves:

        condicao_atual = (
            F.col(chave).isNull()
            | (
                F.trim(
                    F.col(chave).cast("string")
                ) == ""
            )
        )

        if condicao_chave_nula is None:
            condicao_chave_nula = condicao_atual
        else:
            condicao_chave_nula = (
                condicao_chave_nula | condicao_atual
            )

    registros_chave_nula = (
        df.filter(condicao_chave_nula).count()
    )

    grupos_duplicados_df = (
        df.groupBy(*chaves)
        .count()
        .filter(F.col("count") > 1)
    )

    quantidade_grupos_duplicados = (
        grupos_duplicados_df.count()
    )

    resultado_soma = (
        grupos_duplicados_df
        .agg(
            F.sum("count").alias("total")
        )
        .first()
    )

    registros_em_duplicidade = (
        resultado_soma["total"]
        if resultado_soma["total"] is not None
        else 0
    )

    duplicatas_excedentes = (
        registros_em_duplicidade
        - quantidade_grupos_duplicados
    )

    resultados_unicidade.append({
        "tabela": nome_completo,
        "chave_analisada": ", ".join(chaves),
        "total_registros": total_registros,
        "registros_chave_nula": registros_chave_nula,
        "grupos_duplicados": quantidade_grupos_duplicados,
        "registros_em_grupos_duplicados": registros_em_duplicidade,
        "duplicatas_excedentes": duplicatas_excedentes
    })

df_unicidade = spark.createDataFrame(
    resultados_unicidade
)

display(
    df_unicidade.orderBy("tabela")
)

# COMMAND ----------

# ============================================================
# DUPLICIDADE COMPLETA DE REGISTROS
# ============================================================

resultados_duplicidade_completa = []

for tabela in TABELAS_BRONZE:

    nome_completo = (
        f"{CATALOGO}.{SCHEMA_BRONZE}.{tabela}"
    )

    df = spark.table(nome_completo)

    colunas_originais = [
        coluna
        for coluna in df.columns
        if coluna not in colunas_tecnicas
    ]

    total_registros = df.count()

    registros_distintos = (
        df.select(*colunas_originais)
        .distinct()
        .count()
    )

    duplicatas_completas_excedentes = (
        total_registros - registros_distintos
    )

    if total_registros > 0:
        percentual_duplicatas = round(
            duplicatas_completas_excedentes
            * 100.0
            / total_registros,
            4
        )
    else:
        percentual_duplicatas = 0.0

    resultados_duplicidade_completa.append({
        "tabela": nome_completo,
        "total_registros": total_registros,
        "registros_distintos": registros_distintos,
        "duplicatas_completas_excedentes": (
            duplicatas_completas_excedentes
        ),
        "percentual_duplicatas": percentual_duplicatas
    })

df_duplicidade_completa = spark.createDataFrame(
    resultados_duplicidade_completa
)

display(
    df_duplicidade_completa.orderBy("tabela")
)

# COMMAND ----------

# ============================================================
# TABELAS PARA ANÁLISE DE INTEGRIDADE REFERENCIAL
# ============================================================

df_orders = spark.table("workspace.bronze.orders")
df_customers = spark.table("workspace.bronze.customers")
df_order_items = spark.table("workspace.bronze.order_items")
df_order_payments = spark.table("workspace.bronze.order_payments")
df_order_reviews = spark.table("workspace.bronze.order_reviews")
df_products = spark.table("workspace.bronze.products")
df_sellers = spark.table("workspace.bronze.sellers")

print("Tabelas carregadas para análise de integridade: OK")

# COMMAND ----------

# ============================================================
# RELACIONAMENTOS A SEREM VALIDADOS
# ============================================================

testes_integridade = [
    (
        "orders.customer_id -> customers.customer_id",
        df_orders,
        "customer_id",
        df_customers,
        "customer_id"
    ),
    (
        "order_items.order_id -> orders.order_id",
        df_order_items,
        "order_id",
        df_orders,
        "order_id"
    ),
    (
        "order_items.product_id -> products.product_id",
        df_order_items,
        "product_id",
        df_products,
        "product_id"
    ),
    (
        "order_items.seller_id -> sellers.seller_id",
        df_order_items,
        "seller_id",
        df_sellers,
        "seller_id"
    ),
    (
        "order_payments.order_id -> orders.order_id",
        df_order_payments,
        "order_id",
        df_orders,
        "order_id"
    ),
    (
        "order_reviews.order_id -> orders.order_id",
        df_order_reviews,
        "order_id",
        df_orders,
        "order_id"
    )
]

print(
    f"Relacionamentos configurados: "
    f"{len(testes_integridade)}"
)

# COMMAND ----------

# ============================================================
# ANÁLISE DE INTEGRIDADE REFERENCIAL
# ============================================================

resultados_integridade = []

for teste in testes_integridade:

    relacionamento = teste[0]
    df_origem = teste[1]
    campo_origem = teste[2]
    df_destino = teste[3]
    campo_destino = teste[4]

    origem = (
        df_origem
        .select(
            F.col(campo_origem).alias("chave")
        )
        .filter(
            F.col("chave").isNotNull()
        )
        .filter(
            F.trim(
                F.col("chave").cast("string")
            ) != ""
        )
    )

    destino = (
        df_destino
        .select(
            F.col(campo_destino).alias("chave")
        )
        .filter(
            F.col("chave").isNotNull()
        )
        .filter(
            F.trim(
                F.col("chave").cast("string")
            ) != ""
        )
        .distinct()
    )

    total_origem = origem.count()

    sem_correspondencia = (
        origem
        .join(
            destino,
            on="chave",
            how="left_anti"
        )
        .count()
    )

    if total_origem > 0:
        percentual = round(
            sem_correspondencia * 100.0 / total_origem,
            4
        )
    else:
        percentual = 0.0

    if sem_correspondencia == 0:
        status = "OK"
    else:
        status = "VERIFICAR"

    resultados_integridade.append({
        "relacionamento": relacionamento,
        "registros_origem": total_origem,
        "sem_correspondencia": sem_correspondencia,
        "percentual_sem_correspondencia": percentual,
        "status": status
    })

df_integridade = spark.createDataFrame(
    resultados_integridade
)

display(
    df_integridade.orderBy("relacionamento")
)

# COMMAND ----------

# ============================================================
# PREPARAÇÃO TEMPORÁRIA DAS DATAS
# ============================================================

df_orders_datas = (
    df_orders
    .withColumn(
        "purchase_ts",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "approved_ts",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "carrier_ts",
        F.to_timestamp("order_delivered_carrier_date")
    )
    .withColumn(
        "delivered_ts",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "estimated_ts",
        F.to_timestamp("order_estimated_delivery_date")
    )
)

print("Conversão temporária das datas concluída: OK")

# COMMAND ----------

# ============================================================
# FALHAS NA CONVERSÃO DAS DATAS
# ============================================================

pares_datas = [
    (
        "order_purchase_timestamp",
        "purchase_ts"
    ),
    (
        "order_approved_at",
        "approved_ts"
    ),
    (
        "order_delivered_carrier_date",
        "carrier_ts"
    ),
    (
        "order_delivered_customer_date",
        "delivered_ts"
    ),
    (
        "order_estimated_delivery_date",
        "estimated_ts"
    )
]

resultado_conversao_datas = []

for coluna_original, coluna_convertida in pares_datas:

    preenchidos = (
        df_orders_datas
        .filter(F.col(coluna_original).isNotNull())
        .filter(F.trim(F.col(coluna_original)) != "")
        .count()
    )

    falhas = (
        df_orders_datas
        .filter(F.col(coluna_original).isNotNull())
        .filter(F.trim(F.col(coluna_original)) != "")
        .filter(F.col(coluna_convertida).isNull())
        .count()
    )

    resultado_conversao_datas.append({
        "coluna": coluna_original,
        "valores_preenchidos": preenchidos,
        "falhas_conversao": falhas
    })

df_conversao_datas = spark.createDataFrame(
    resultado_conversao_datas
)

display(
    df_conversao_datas.orderBy("coluna")
)

# COMMAND ----------

# ============================================================
# CONSISTÊNCIA TEMPORAL DOS PEDIDOS
# ============================================================

resultado_datas = []

resultado_datas.append({
    "regra": "Aprovacao anterior a compra",
    "registros": (
        df_orders_datas
        .filter(
            F.col("approved_ts") < F.col("purchase_ts")
        )
        .count()
    ),
    "classificacao": "Inconsistencia potencial"
})

resultado_datas.append({
    "regra": "Envio anterior a compra",
    "registros": (
        df_orders_datas
        .filter(
            F.col("carrier_ts") < F.col("purchase_ts")
        )
        .count()
    ),
    "classificacao": "Inconsistencia potencial"
})

resultado_datas.append({
    "regra": "Entrega anterior a compra",
    "registros": (
        df_orders_datas
        .filter(
            F.col("delivered_ts") < F.col("purchase_ts")
        )
        .count()
    ),
    "classificacao": "Inconsistencia potencial"
})

resultado_datas.append({
    "regra": "Entrega anterior ao envio",
    "registros": (
        df_orders_datas
        .filter(
            F.col("delivered_ts") < F.col("carrier_ts")
        )
        .count()
    ),
    "classificacao": "Inconsistencia potencial"
})

resultado_datas.append({
    "regra": "Entrega posterior a data estimada",
    "registros": (
        df_orders_datas
        .filter(
            F.col("delivered_ts") > F.col("estimated_ts")
        )
        .count()
    ),
    "classificacao": "Evento de negocio: atraso"
})

df_resultado_datas = spark.createDataFrame(
    resultado_datas
)

display(
    df_resultado_datas.orderBy("regra")
)

# COMMAND ----------

# ============================================================
# DATAS AUSENTES POR STATUS DO PEDIDO
# ============================================================

display(
    df_orders
    .groupBy("order_status")
    .agg(
        F.count("*").alias("total_pedidos"),
        F.sum(
            F.when(
                F.col(
                    "order_delivered_customer_date"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("sem_data_entrega"),
        F.sum(
            F.when(
                F.col(
                    "order_delivered_carrier_date"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("sem_data_transportadora"),
        F.sum(
            F.when(
                F.col("order_approved_at").isNull(),
                1
            ).otherwise(0)
        ).alias("sem_data_aprovacao")
    )
    .orderBy(F.desc("total_pedidos"))
)

# COMMAND ----------

# ============================================================
# CONVERSÃO TEMPORÁRIA DE PREÇO E FRETE
# ============================================================

df_itens_numericos = (
    df_order_items
    .withColumn(
        "price_num",
        F.col("price").cast("double")
    )
    .withColumn(
        "freight_num",
        F.col("freight_value").cast("double")
    )
)

print("Conversão temporária de preço e frete concluída: OK")

# COMMAND ----------

# ============================================================
# QUALIDADE DOS CAMPOS NUMÉRICOS
# ============================================================

resultado_numericos = []

resultado_numericos.append({
    "regra": "Preco preenchido sem conversao valida",
    "registros": (
        df_itens_numericos
        .filter(F.col("price").isNotNull())
        .filter(F.trim(F.col("price")) != "")
        .filter(F.col("price_num").isNull())
        .count()
    )
})

resultado_numericos.append({
    "regra": "Frete preenchido sem conversao valida",
    "registros": (
        df_itens_numericos
        .filter(F.col("freight_value").isNotNull())
        .filter(F.trim(F.col("freight_value")) != "")
        .filter(F.col("freight_num").isNull())
        .count()
    )
})

resultado_numericos.append({
    "regra": "Preco negativo",
    "registros": (
        df_itens_numericos
        .filter(F.col("price_num") < 0)
        .count()
    )
})

resultado_numericos.append({
    "regra": "Frete negativo",
    "registros": (
        df_itens_numericos
        .filter(F.col("freight_num") < 0)
        .count()
    )
})

resultado_numericos.append({
    "regra": "Preco igual a zero",
    "registros": (
        df_itens_numericos
        .filter(F.col("price_num") == 0)
        .count()
    )
})

resultado_numericos.append({
    "regra": "Frete igual a zero",
    "registros": (
        df_itens_numericos
        .filter(F.col("freight_num") == 0)
        .count()
    )
})

df_resultado_numericos = spark.createDataFrame(
    resultado_numericos
)

display(
    df_resultado_numericos.orderBy("regra")
)

# COMMAND ----------

# ============================================================
# ESTATÍSTICAS DE PREÇO E FRETE
# ============================================================

display(
    df_itens_numericos
    .select(
        "price_num",
        "freight_num"
    )
    .summary(
        "count",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "mean",
        "stddev"
    )
)

# COMMAND ----------

# ============================================================
# POSSÍVEIS OUTLIERS PELO MÉTODO IQR
# ============================================================

quantis_preco = (
    df_itens_numericos
    .approxQuantile(
        "price_num",
        [0.25, 0.75],
        0.001
    )
)

quantis_frete = (
    df_itens_numericos
    .approxQuantile(
        "freight_num",
        [0.25, 0.75],
        0.001
    )
)

q1_preco = quantis_preco[0]
q3_preco = quantis_preco[1]
iqr_preco = q3_preco - q1_preco
limite_preco = q3_preco + 1.5 * iqr_preco

q1_frete = quantis_frete[0]
q3_frete = quantis_frete[1]
iqr_frete = q3_frete - q1_frete
limite_frete = q3_frete + 1.5 * iqr_frete

possiveis_outliers_preco = (
    df_itens_numericos
    .filter(F.col("price_num") > limite_preco)
    .count()
)

possiveis_outliers_frete = (
    df_itens_numericos
    .filter(F.col("freight_num") > limite_frete)
    .count()
)

resultado_outliers = [
    {
        "campo": "price",
        "q1": float(q1_preco),
        "q3": float(q3_preco),
        "limite_superior_iqr": float(limite_preco),
        "registros_acima_limite": possiveis_outliers_preco
    },
    {
        "campo": "freight_value",
        "q1": float(q1_frete),
        "q3": float(q3_frete),
        "limite_superior_iqr": float(limite_frete),
        "registros_acima_limite": possiveis_outliers_frete
    }
]

df_resultado_outliers = spark.createDataFrame(
    resultado_outliers
)

display(df_resultado_outliers)

# COMMAND ----------

# ============================================================
# RESUMO CONSOLIDADO DA QUALIDADE
# ============================================================

total_ausentes = (
    df_completude
    .agg(
        F.sum("valores_ausentes").alias("total")
    )
    .first()["total"]
)

total_duplicatas = (
    df_duplicidade_completa
    .agg(
        F.sum(
            "duplicatas_completas_excedentes"
        ).alias("total")
    )
    .first()["total"]
)

total_sem_correspondencia = (
    df_integridade
    .agg(
        F.sum(
            "sem_correspondencia"
        ).alias("total")
    )
    .first()["total"]
)

total_falhas_datas = (
    df_conversao_datas
    .agg(
        F.sum("falhas_conversao").alias("total")
    )
    .first()["total"]
)

resumo_qualidade = [
    {
        "dimensao": "Completude",
        "indicador": "Valores ausentes",
        "resultado": int(total_ausentes or 0)
    },
    {
        "dimensao": "Unicidade",
        "indicador": "Duplicatas completas excedentes",
        "resultado": int(total_duplicatas or 0)
    },
    {
        "dimensao": "Integridade",
        "indicador": "Registros sem correspondencia",
        "resultado": int(
            total_sem_correspondencia or 0
        )
    },
    {
        "dimensao": "Consistencia",
        "indicador": "Falhas na conversao de datas",
        "resultado": int(total_falhas_datas or 0)
    }
]

df_resumo_qualidade = spark.createDataFrame(
    resumo_qualidade
)

display(
    df_resumo_qualidade.orderBy(
        "dimensao",
        "indicador"
    )
)

# COMMAND ----------

# ============================================================
# ENCERRAMENTO
# ============================================================

print("=" * 60)
print("ANÁLISE DE QUALIDADE DA BRONZE CONCLUÍDA")
print("=" * 60)
print(f"Tabelas analisadas: {len(TABELAS_BRONZE)}")
print("Completude: analisada")
print("Unicidade: analisada")
print("Duplicidade completa: analisada")
print("Integridade referencial: analisada")
print("Consistência temporal: analisada")
print("Campos numéricos: analisados")
print("Possíveis outliers: analisados")
print("Dados alterados na Bronze: NÃO")
print("=" * 60)