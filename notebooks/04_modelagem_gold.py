# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Modelagem da Camada Gold
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Construir um modelo dimensional em esquema estrela para disponibilizar
# MAGIC os dados tratados da camada Silver em estruturas adequadas às análises
# MAGIC de negócio.
# MAGIC
# MAGIC ## Modelo criado
# MAGIC
# MAGIC ### Dimensões
# MAGIC
# MAGIC - `dim_cliente`: características geográficas dos clientes;
# MAGIC - `dim_produto`: categorias e atributos dos produtos;
# MAGIC - `dim_vendedor`: localização dos vendedores;
# MAGIC - `dim_tempo`: atributos de calendário para análises temporais.
# MAGIC
# MAGIC ### Tabelas fato
# MAGIC
# MAGIC - `fato_pedidos`: uma linha por pedido;
# MAGIC - `fato_itens_pedido`: uma linha por item de pedido.
# MAGIC
# MAGIC ## Origem
# MAGIC
# MAGIC `workspace.silver`
# MAGIC
# MAGIC ## Destino
# MAGIC
# MAGIC `workspace.gold`
# MAGIC
# MAGIC Pagamentos e avaliações são agregados previamente por pedido para evitar
# MAGIC duplicação de registros na tabela `fato_pedidos`.

# COMMAND ----------

# ============================================================
# CONFIGURACAO DA CAMADA GOLD
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOGO = "workspace"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

resultados_gold = []

print("=" * 60)
print("MODELAGEM DA CAMADA GOLD")
print("=" * 60)
print(f"Origem: {CATALOGO}.{SCHEMA_SILVER}")
print(f"Destino: {CATALOGO}.{SCHEMA_GOLD}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# FUNCAO PARA GRAVAR TABELAS GOLD
# ============================================================

def gravar_gold(df, nome_tabela, granularidade):

    tabela_destino = (
        f"{CATALOGO}.{SCHEMA_GOLD}.{nome_tabela}"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_destino)
    )

    registros = df.count()
    colunas = len(df.columns)

    resultados_gold.append({
        "tabela": tabela_destino,
        "granularidade": granularidade,
        "registros": registros,
        "colunas": colunas,
        "status": "OK"
    })

    print(
        f"{tabela_destino}: "
        f"{registros} registros, "
        f"{colunas} colunas"
    )

# COMMAND ----------

# ============================================================
# CARREGAR TABELAS SILVER
# ============================================================

df_customers = spark.table(
    "workspace.silver.customers"
)

df_products = spark.table(
    "workspace.silver.products"
)

df_sellers = spark.table(
    "workspace.silver.sellers"
)

df_orders = spark.table(
    "workspace.silver.orders"
)

df_items = spark.table(
    "workspace.silver.order_items"
)

df_payments = spark.table(
    "workspace.silver.order_payments"
)

df_reviews = spark.table(
    "workspace.silver.order_reviews"
)

print("Tabelas Silver carregadas: OK")

# COMMAND ----------

# ============================================================
# DIMENSAO CLIENTE
# Granularidade: uma linha por customer_id
# ============================================================

dim_cliente = (
    df_customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    )
    .dropDuplicates(["customer_id"])
    .withColumn(
        "_gold_timestamp",
        F.current_timestamp()
    )
)

gravar_gold(
    dim_cliente,
    "dim_cliente",
    "Uma linha por customer_id"
)

# COMMAND ----------

# ============================================================
# DIMENSAO PRODUTO
# Granularidade: uma linha por product_id
# ============================================================

dim_produto = (
    df_products
    .select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3"
    )
    .dropDuplicates(["product_id"])
    .withColumn(
        "_gold_timestamp",
        F.current_timestamp()
    )
)

gravar_gold(
    dim_produto,
    "dim_produto",
    "Uma linha por product_id"
)

# COMMAND ----------

# ============================================================
# DIMENSAO VENDEDOR
# Granularidade: uma linha por seller_id
# ============================================================

dim_vendedor = (
    df_sellers
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    )
    .dropDuplicates(["seller_id"])
    .withColumn(
        "_gold_timestamp",
        F.current_timestamp()
    )
)

gravar_gold(
    dim_vendedor,
    "dim_vendedor",
    "Uma linha por seller_id"
)

# COMMAND ----------

# ============================================================
# INTERVALO DA DIMENSAO TEMPO
# ============================================================

intervalo_datas = (
    df_orders
    .agg(
        F.min("purchase_date").alias("data_minima"),
        F.max("purchase_date").alias("data_maxima")
    )
    .first()
)

DATA_MINIMA = intervalo_datas["data_minima"]
DATA_MAXIMA = intervalo_datas["data_maxima"]

print(f"Data minima: {DATA_MINIMA}")
print(f"Data maxima: {DATA_MAXIMA}")

# COMMAND ----------

# ============================================================
# DIMENSAO TEMPO
# Granularidade: uma linha por data
# ============================================================

quantidade_dias = (
    DATA_MAXIMA - DATA_MINIMA
).days + 1

dim_tempo = (
    spark.range(0, quantidade_dias)
    .select(
        F.date_add(
            F.lit(DATA_MINIMA),
            F.col("id").cast("int")
        ).alias("data")
    )
    .withColumn(
        "data_key",
        F.date_format("data", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "ano",
        F.year("data")
    )
    .withColumn(
        "mes_numero",
        F.month("data")
    )
    .withColumn(
        "mes_nome",
        F.date_format("data", "MMMM")
    )
    .withColumn(
        "ano_mes",
        F.date_format("data", "yyyy-MM")
    )
    .withColumn(
        "trimestre",
        F.quarter("data")
    )
    .withColumn(
        "dia_mes",
        F.dayofmonth("data")
    )
    .withColumn(
        "dia_semana_numero",
        F.dayofweek("data")
    )
    .withColumn(
        "dia_semana_nome",
        F.date_format("data", "EEEE")
    )
    .withColumn(
        "semana_ano",
        F.weekofyear("data")
    )
    .withColumn(
        "fim_de_semana",
        F.dayofweek("data").isin([1, 7])
    )
    .withColumn(
        "_gold_timestamp",
        F.current_timestamp()
    )
)

gravar_gold(
    dim_tempo,
    "dim_tempo",
    "Uma linha por data"
)

# COMMAND ----------

# ============================================================
# AGREGACAO DOS PAGAMENTOS POR PEDIDO
# ============================================================

pagamentos_por_pedido = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum("payment_value")
        .cast(T.DecimalType(18, 2))
        .alias("payment_total_value"),

        F.max("payment_installments")
        .alias("max_payment_installments"),

        F.count("*")
        .alias("payment_record_count"),

        F.countDistinct("payment_type")
        .alias("payment_type_count"),

        F.concat_ws(
            ", ",
            F.sort_array(
                F.collect_set("payment_type")
            )
        ).alias("payment_types")
    )
)

display(
    pagamentos_por_pedido.limit(10)
)

# COMMAND ----------

# ============================================================
# AGREGACAO DAS AVALIACOES POR PEDIDO
# ============================================================

avaliacoes_por_pedido = (
    df_reviews
    .groupBy("order_id")
    .agg(
        F.round(
            F.avg("review_score"),
            2
        ).alias("average_review_score"),

        F.min("review_score")
        .alias("minimum_review_score"),

        F.max("review_score")
        .alias("maximum_review_score"),

        F.count("*")
        .alias("review_record_count"),

        F.sum(
            F.when(
                F.col(
                    "review_comment_message"
                ).isNotNull(),
                1
            ).otherwise(0)
        ).alias("review_comment_count")
    )
)

display(
    avaliacoes_por_pedido.limit(10)
)

# COMMAND ----------

# ============================================================
# AGREGACAO DOS ITENS POR PEDIDO
# ============================================================

itens_por_pedido = (
    df_items
    .groupBy("order_id")
    .agg(
        F.count("*")
        .alias("item_count"),

        F.countDistinct("product_id")
        .alias("distinct_product_count"),

        F.countDistinct("seller_id")
        .alias("distinct_seller_count"),

        F.sum("price")
        .cast(T.DecimalType(18, 2))
        .alias("product_total_value"),

        F.sum("freight_value")
        .cast(T.DecimalType(18, 2))
        .alias("freight_total_value"),

        F.sum("item_total_value")
        .cast(T.DecimalType(18, 2))
        .alias("order_items_total_value")
    )
)

display(
    itens_por_pedido.limit(10)
)

# COMMAND ----------

# ============================================================
# VALIDACAO DAS AGREGACOES POR PEDIDO
# ============================================================

print(
    "Pedidos com pagamentos agregados:",
    pagamentos_por_pedido.count()
)

print(
    "Pedidos com avaliacoes agregadas:",
    avaliacoes_por_pedido.count()
)

print(
    "Pedidos com itens agregados:",
    itens_por_pedido.count()
)

print("Agregacoes preparadas: OK")

# COMMAND ----------

# ============================================================
# FATO PEDIDOS
# Granularidade: uma linha por order_id
# ============================================================

fato_pedidos = (
    df_orders
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "purchase_date",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "delay_days",
        "is_delayed"
    )
    .join(
        itens_por_pedido,
        on="order_id",
        how="left"
    )
    .join(
        pagamentos_por_pedido,
        on="order_id",
        how="left"
    )
    .join(
        avaliacoes_por_pedido,
        on="order_id",
        how="left"
    )
    .withColumn(
        "purchase_date_key",
        F.date_format(
            "purchase_date",
            "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "_gold_timestamp",
        F.current_timestamp()
    )
)

print("DataFrame fato_pedidos criado: OK")

# COMMAND ----------

# ============================================================
# VALIDAR GRANULARIDADE DA FATO PEDIDOS
# ============================================================

total_fato_pedidos = fato_pedidos.count()

pedidos_distintos = (
    fato_pedidos
    .select("order_id")
    .distinct()
    .count()
)

duplicatas_fato_pedidos = (
    total_fato_pedidos - pedidos_distintos
)

print(f"Registros: {total_fato_pedidos}")
print(f"Pedidos distintos: {pedidos_distintos}")
print(f"Duplicatas excedentes: {duplicatas_fato_pedidos}")

if duplicatas_fato_pedidos == 0:
    print("Granularidade da fato_pedidos: OK")
else:
    print("Granularidade da fato_pedidos: VERIFICAR")
    

# COMMAND ----------

# ============================================================
# GRAVAR FATO PEDIDOS
# ============================================================

gravar_gold(
    fato_pedidos,
    "fato_pedidos",
    "Uma linha por order_id"
)

# COMMAND ----------

# ============================================================
# AMOSTRA DA FATO PEDIDOS
# ============================================================

display(
    fato_pedidos
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "purchase_date",
        "item_count",
        "product_total_value",
        "freight_total_value",
        "payment_total_value",
        "average_review_score",
        "delivery_days",
        "delay_days",
        "is_delayed"
    )
    .limit(10)
)

# COMMAND ----------

# ============================================================
# FATO ITENS PEDIDO
# Granularidade: order_id e order_item_id
# ============================================================

fato_itens_pedido = (
    df_items
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "item_total_value",
        "freight_percentage"
    )
    .join(
        df_orders.select(
            "order_id",
            "customer_id",
            "purchase_date",
            "order_status",
            "is_delayed",
            "delay_days"
        ),
        on="order_id",
        how="left"
    )
    .withColumn(
        "purchase_date_key",
        F.date_format(
            "purchase_date",
            "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "_gold_timestamp",
        F.current_timestamp()
    )
)

print("DataFrame fato_itens_pedido criado: OK")

# COMMAND ----------

# ============================================================
# VALIDAR GRANULARIDADE DA FATO ITENS
# ============================================================

total_fato_itens = fato_itens_pedido.count()

itens_distintos = (
    fato_itens_pedido
    .select(
        "order_id",
        "order_item_id"
    )
    .distinct()
    .count()
)

duplicatas_fato_itens = (
    total_fato_itens - itens_distintos
)

print(f"Registros: {total_fato_itens}")
print(f"Chaves distintas: {itens_distintos}")
print(f"Duplicatas excedentes: {duplicatas_fato_itens}")

if duplicatas_fato_itens == 0:
    print("Granularidade da fato_itens_pedido: OK")
else:
    print("Granularidade da fato_itens_pedido: VERIFICAR")

# COMMAND ----------

# ============================================================
# GRAVAR FATO ITENS PEDIDO
# ============================================================

gravar_gold(
    fato_itens_pedido,
    "fato_itens_pedido",
    "Uma linha por order_id e order_item_id"
)

# COMMAND ----------

# ============================================================
# RESUMO DAS TABELAS GOLD
# ============================================================

df_resultados_gold = spark.createDataFrame(
    resultados_gold
)

display(
    df_resultados_gold.orderBy("tabela")
)

# COMMAND ----------

# ============================================================
# LISTAR TABELAS GOLD
# ============================================================

display(
    spark.sql(
        "SHOW TABLES IN workspace.gold"
    )
)

# COMMAND ----------

# ============================================================
# VALIDACAO DAS GRANULARIDADES
# ============================================================

validacao_granularidade = []

total_fato_pedidos = fato_pedidos.count()

pedidos_distintos = (
    fato_pedidos
    .select("order_id")
    .distinct()
    .count()
)

validacao_granularidade.append({
    "tabela": "workspace.gold.fato_pedidos",
    "granularidade": "order_id",
    "registros": total_fato_pedidos,
    "chaves_distintas": pedidos_distintos,
    "duplicatas_excedentes": (
        total_fato_pedidos - pedidos_distintos
    )
})

total_fato_itens = fato_itens_pedido.count()

itens_distintos = (
    fato_itens_pedido
    .select(
        "order_id",
        "order_item_id"
    )
    .distinct()
    .count()
)

validacao_granularidade.append({
    "tabela": "workspace.gold.fato_itens_pedido",
    "granularidade": "order_id + order_item_id",
    "registros": total_fato_itens,
    "chaves_distintas": itens_distintos,
    "duplicatas_excedentes": (
        total_fato_itens - itens_distintos
    )
})

df_validacao_granularidade = spark.createDataFrame(
    validacao_granularidade
)

display(df_validacao_granularidade)


# COMMAND ----------

# ============================================================
# VALIDACAO DAS DIMENSOES
# ============================================================

validacao_dimensoes = []

dimensoes = [
    ("dim_cliente", "customer_id"),
    ("dim_produto", "product_id"),
    ("dim_vendedor", "seller_id"),
    ("dim_tempo", "data_key")
]

for tabela, chave in dimensoes:

    df_dimensao = spark.table(
        f"workspace.gold.{tabela}"
    )

    total = df_dimensao.count()

    chaves_distintas = (
        df_dimensao
        .select(chave)
        .distinct()
        .count()
    )

    chaves_nulas = (
        df_dimensao
        .filter(F.col(chave).isNull())
        .count()
    )

    duplicatas = total - chaves_distintas

    if duplicatas == 0 and chaves_nulas == 0:
        status = "OK"
    else:
        status = "VERIFICAR"

    validacao_dimensoes.append({
        "tabela": f"workspace.gold.{tabela}",
        "chave": chave,
        "registros": total,
        "chaves_distintas": chaves_distintas,
        "chaves_nulas": chaves_nulas,
        "duplicatas_excedentes": duplicatas,
        "status": status
    })

df_validacao_dimensoes = spark.createDataFrame(
    validacao_dimensoes
)

display(
    df_validacao_dimensoes.orderBy("tabela")
)

# COMMAND ----------

# ============================================================
# INTEGRIDADE REFERENCIAL DA GOLD
# ============================================================

relacionamentos_gold = [
    (
        "fato_pedidos.customer_id -> dim_cliente.customer_id",
        fato_pedidos,
        "customer_id",
        dim_cliente,
        "customer_id"
    ),
    (
        "fato_pedidos.purchase_date_key -> dim_tempo.data_key",
        fato_pedidos,
        "purchase_date_key",
        dim_tempo,
        "data_key"
    ),
    (
        "fato_itens.product_id -> dim_produto.product_id",
        fato_itens_pedido,
        "product_id",
        dim_produto,
        "product_id"
    ),
    (
        "fato_itens.seller_id -> dim_vendedor.seller_id",
        fato_itens_pedido,
        "seller_id",
        dim_vendedor,
        "seller_id"
    ),
    (
        "fato_itens.purchase_date_key -> dim_tempo.data_key",
        fato_itens_pedido,
        "purchase_date_key",
        dim_tempo,
        "data_key"
    )
]

resultado_relacionamentos_gold = []

for item in relacionamentos_gold:

    nome = item[0]
    origem = item[1]
    chave_origem = item[2]
    destino = item[3]
    chave_destino = item[4]

    chaves_origem = (
        origem
        .select(
            F.col(chave_origem).alias("chave")
        )
        .filter(F.col("chave").isNotNull())
    )

    chaves_destino = (
        destino
        .select(
            F.col(chave_destino).alias("chave")
        )
        .filter(F.col("chave").isNotNull())
        .distinct()
    )

    sem_correspondencia = (
        chaves_origem
        .join(
            chaves_destino,
            on="chave",
            how="left_anti"
        )
        .count()
    )

    if sem_correspondencia == 0:
        status = "OK"
    else:
        status = "VERIFICAR"

    resultado_relacionamentos_gold.append({
        "relacionamento": nome,
        "sem_correspondencia": sem_correspondencia,
        "status": status
    })

df_integridade_gold = spark.createDataFrame(
    resultado_relacionamentos_gold
)

display(
    df_integridade_gold.orderBy("relacionamento")
)

# COMMAND ----------

# ============================================================
# RECONCILIACAO DOS VALORES MONETARIOS
# ============================================================

totais_silver = (
    spark.table("workspace.silver.order_items")
    .agg(
        F.round(
            F.sum("price"),
            2
        ).alias("total_price"),

        F.round(
            F.sum("freight_value"),
            2
        ).alias("total_freight"),

        F.round(
            F.sum("item_total_value"),
            2
        ).alias("total_items")
    )
    .first()
)

totais_gold = (
    spark.table("workspace.gold.fato_itens_pedido")
    .agg(
        F.round(
            F.sum("price"),
            2
        ).alias("total_price"),

        F.round(
            F.sum("freight_value"),
            2
        ).alias("total_freight"),

        F.round(
            F.sum("item_total_value"),
            2
        ).alias("total_items")
    )
    .first()
)

reconciliacao_valores = [
    {
        "metrica": "Total de produtos",
        "valor_silver": float(
            totais_silver["total_price"]
        ),
        "valor_gold": float(
            totais_gold["total_price"]
        )
    },
    {
        "metrica": "Total de frete",
        "valor_silver": float(
            totais_silver["total_freight"]
        ),
        "valor_gold": float(
            totais_gold["total_freight"]
        )
    },
    {
        "metrica": "Total dos itens",
        "valor_silver": float(
            totais_silver["total_items"]
        ),
        "valor_gold": float(
            totais_gold["total_items"]
        )
    }
]

df_reconciliacao_valores = (
    spark.createDataFrame(
        reconciliacao_valores
    )
    .withColumn(
        "diferenca",
        F.round(
            F.col("valor_gold")
            - F.col("valor_silver"),
            2
        )
    )
    .withColumn(
        "status",
        F.when(
            F.col("diferenca") == 0,
            F.lit("OK")
        ).otherwise(
            F.lit("VERIFICAR")
        )
    )
)

display(df_reconciliacao_valores)

# COMMAND ----------

# ============================================================
# ENCERRAMENTO DA MODELAGEM GOLD
# ============================================================

duplicatas_fatos = sum(
    item["duplicatas_excedentes"]
    for item in validacao_granularidade
)

dimensoes_com_erro = [
    item
    for item in validacao_dimensoes
    if item["status"] != "OK"
]

relacionamentos_com_erro = [
    item
    for item in resultado_relacionamentos_gold
    if item["status"] != "OK"
]

metricas_monetarias_com_erro = (
    df_reconciliacao_valores
    .filter(F.col("status") != "OK")
    .count()
)

print("=" * 60)
print("MODELAGEM DA CAMADA GOLD CONCLUIDA")
print("=" * 60)
print("Tabelas criadas: 6")
print("Dimensoes criadas: 4")
print("Tabelas fato criadas: 2")
print(
    f"Duplicatas nas chaves das fatos: "
    f"{duplicatas_fatos}"
)
print(
    f"Dimensoes com erro: "
    f"{len(dimensoes_com_erro)}"
)
print(
    f"Relacionamentos com erro: "
    f"{len(relacionamentos_com_erro)}"
)
print(
    f"Metricas monetarias com erro: "
    f"{metricas_monetarias_com_erro}"
)
print("Modelo dimensional: ESQUEMA ESTRELA")

if (
    duplicatas_fatos == 0
    and not dimensoes_com_erro
    and not relacionamentos_com_erro
    and metricas_monetarias_com_erro == 0
):
    print("Status geral: OK")
else:
    print("Status geral: VERIFICAR")

print("=" * 60)