# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# ============================================================
# CONFIGURACAO DA CAMADA SILVER
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOGO = "workspace"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

print("=" * 60)
print("TRANSFORMACAO DA CAMADA SILVER")
print("=" * 60)
print(f"Origem: {CATALOGO}.{SCHEMA_BRONZE}")
print(f"Destino: {CATALOGO}.{SCHEMA_SILVER}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# FUNCAO AUXILIAR PARA GRAVAR TABELAS SILVER
# ============================================================

resultados_silver = []

def gravar_silver(df, nome_tabela):

    tabela_destino = (
        f"{CATALOGO}.{SCHEMA_SILVER}.{nome_tabela}"
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

    resultados_silver.append({
        "tabela": tabela_destino,
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
# SILVER CUSTOMERS
# ============================================================

df_customers_bronze = spark.table(
    "workspace.bronze.customers"
)

df_customers_silver = (
    df_customers_bronze
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias(
            "customer_unique_id"
        ),
        F.col("customer_zip_code_prefix")
        .cast("int")
        .alias("customer_zip_code_prefix"),
        F.lower(
            F.trim("customer_city")
        ).alias("customer_city"),
        F.upper(
            F.trim("customer_state")
        ).alias("customer_state"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_customers_silver,
    "customers"
)

# COMMAND ----------

# ============================================================
# SILVER SELLERS
# ============================================================

df_sellers_bronze = spark.table(
    "workspace.bronze.sellers"
)

df_sellers_silver = (
    df_sellers_bronze
    .select(
        F.trim("seller_id").alias("seller_id"),
        F.col("seller_zip_code_prefix")
        .cast("int")
        .alias("seller_zip_code_prefix"),
        F.lower(
            F.trim("seller_city")
        ).alias("seller_city"),
        F.upper(
            F.trim("seller_state")
        ).alias("seller_state"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_sellers_silver,
    "sellers"
)

# COMMAND ----------

# ============================================================
# SILVER CATEGORY TRANSLATION
# ============================================================

df_category_bronze = spark.table(
    "workspace.bronze.category_translation"
)

df_category_silver = (
    df_category_bronze
    .select(
        F.lower(
            F.trim("product_category_name")
        ).alias("product_category_name"),
        F.lower(
            F.trim(
                "product_category_name_english"
            )
        ).alias("product_category_name_english"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_category_silver,
    "category_translation"
)

# COMMAND ----------

# ============================================================
# PREPARACAO DOS PRODUTOS
# ============================================================

df_products_bronze = spark.table(
    "workspace.bronze.products"
)

df_products_base = (
    df_products_bronze
    .select(
        F.trim("product_id").alias("product_id"),
        F.lower(
            F.trim("product_category_name")
        ).alias("product_category_name"),
        F.col("product_name_lenght")
        .cast("int")
        .alias("product_name_length"),
        F.col("product_description_lenght")
        .cast("int")
        .alias("product_description_length"),
        F.col("product_photos_qty")
        .cast("int")
        .alias("product_photos_qty"),
        F.col("product_weight_g")
        .cast("double")
        .alias("product_weight_g"),
        F.col("product_length_cm")
        .cast("double")
        .alias("product_length_cm"),
        F.col("product_height_cm")
        .cast("double")
        .alias("product_height_cm"),
        F.col("product_width_cm")
        .cast("double")
        .alias("product_width_cm"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
)

# COMMAND ----------

# ============================================================
# ENRIQUECIMENTO E CALCULOS DOS PRODUTOS
# ============================================================

df_translation_lookup = (
    df_category_silver
    .select(
        "product_category_name",
        "product_category_name_english"
    )
)

df_products_silver = (
    df_products_base
    .join(
        df_translation_lookup,
        on="product_category_name",
        how="left"
    )
    .withColumn(
        "product_category_name",
        F.coalesce(
            F.col("product_category_name"),
            F.lit("nao_informado")
        )
    )
    .withColumn(
        "product_category_name_english",
        F.coalesce(
            F.col("product_category_name_english"),
            F.col("product_category_name")
        )
    )
    .withColumn(
        "product_volume_cm3",
        F.when(
            F.col("product_length_cm").isNotNull()
            & F.col("product_height_cm").isNotNull()
            & F.col("product_width_cm").isNotNull(),
            F.col("product_length_cm")
            * F.col("product_height_cm")
            * F.col("product_width_cm")
        )
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_products_silver,
    "products"
)

# COMMAND ----------

# ============================================================
# SILVER ORDERS
# ============================================================

df_orders_bronze = spark.table(
    "workspace.bronze.orders"
)

df_orders_silver = (
    df_orders_bronze
    .select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(
            F.trim("order_status")
        ).alias("order_status"),
        F.to_timestamp(
            "order_purchase_timestamp"
        ).alias("order_purchase_timestamp"),
        F.to_timestamp(
            "order_approved_at"
        ).alias("order_approved_at"),
        F.to_timestamp(
            "order_delivered_carrier_date"
        ).alias("order_delivered_carrier_date"),
        F.to_timestamp(
            "order_delivered_customer_date"
        ).alias("order_delivered_customer_date"),
        F.to_timestamp(
            "order_estimated_delivery_date"
        ).alias("order_estimated_delivery_date"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
    .withColumn(
        "purchase_date",
        F.to_date("order_purchase_timestamp")
    )
    .withColumn(
        "delivery_days",
        F.datediff(
            "order_delivered_customer_date",
            "order_purchase_timestamp"
        )
    )
    .withColumn(
        "delay_days",
        F.when(
            F.col(
                "order_delivered_customer_date"
            ).isNotNull()
            & F.col(
                "order_estimated_delivery_date"
            ).isNotNull(),
            F.greatest(
                F.datediff(
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ),
                F.lit(0)
            )
        )
    )
    .withColumn(
        "is_delayed",
        F.when(
            F.col(
                "order_delivered_customer_date"
            ).isNull(),
            F.lit(None).cast("boolean")
        ).otherwise(
            F.col(
                "order_delivered_customer_date"
            )
            > F.col(
                "order_estimated_delivery_date"
            )
        )
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_orders_silver,
    "orders"
)

# COMMAND ----------

# ============================================================
# SILVER ORDER ITEMS
# ============================================================

df_items_bronze = spark.table(
    "workspace.bronze.order_items"
)

df_items_silver = (
    df_items_bronze
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("order_item_id")
        .cast("int")
        .alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        F.to_timestamp(
            "shipping_limit_date"
        ).alias("shipping_limit_date"),
        F.col("price")
        .cast(T.DecimalType(18, 2))
        .alias("price"),
        F.col("freight_value")
        .cast(T.DecimalType(18, 2))
        .alias("freight_value"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
    .withColumn(
        "item_total_value",
        (
            F.col("price")
            + F.col("freight_value")
        ).cast(T.DecimalType(18, 2))
    )
    .withColumn(
        "freight_percentage",
        F.when(
            F.col("item_total_value") > 0,
            F.round(
                F.col("freight_value")
                * F.lit(100.0)
                / F.col("item_total_value"),
                4
            )
        )
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_items_silver,
    "order_items"
)

# COMMAND ----------

# ============================================================
# SILVER ORDER PAYMENTS
# ============================================================

df_payments_bronze = spark.table(
    "workspace.bronze.order_payments"
)

df_payments_silver = (
    df_payments_bronze
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("payment_sequential")
        .cast("int")
        .alias("payment_sequential"),
        F.lower(
            F.trim("payment_type")
        ).alias("payment_type"),
        F.col("payment_installments")
        .cast("int")
        .alias("payment_installments"),
        F.col("payment_value")
        .cast(T.DecimalType(18, 2))
        .alias("payment_value"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_payments_silver,
    "order_payments"
)

# COMMAND ----------

# ============================================================
# SILVER ORDER REVIEWS
# ============================================================

df_reviews_bronze = spark.table(
    "workspace.bronze.order_reviews"
)

df_reviews_silver = (
    df_reviews_bronze
    .select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        F.col("review_score")
        .cast("int")
        .alias("review_score"),
        F.when(
            F.trim("review_comment_title") == "",
            F.lit(None)
        ).otherwise(
            F.trim("review_comment_title")
        ).alias("review_comment_title"),
        F.when(
            F.trim("review_comment_message") == "",
            F.lit(None)
        ).otherwise(
            F.trim("review_comment_message")
        ).alias("review_comment_message"),
        F.to_timestamp(
            "review_creation_date"
        ).alias("review_creation_date"),
        F.to_timestamp(
            "review_answer_timestamp"
        ).alias("review_answer_timestamp"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_ingestion_date")
    )
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp()
    )
)

gravar_silver(
    df_reviews_silver,
    "order_reviews"
)

# COMMAND ----------

# ============================================================
# RESUMO DA CAMADA SILVER
# ============================================================

df_resultados_silver = spark.createDataFrame(
    resultados_silver
)

display(
    df_resultados_silver.orderBy("tabela")
)

# COMMAND ----------

# ============================================================
# LISTAR TABELAS SILVER
# ============================================================

display(
    spark.sql(
        "SHOW TABLES IN workspace.silver"
    )
)

# COMMAND ----------

# ============================================================
# RECONCILIACAO BRONZE VERSUS SILVER
# ============================================================

tabelas_comparacao = [
    "category_translation",
    "customers",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers"
]

comparacao_camadas = []

for tabela in tabelas_comparacao:

    registros_bronze = (
        spark.table(
            f"workspace.bronze.{tabela}"
        )
        .count()
    )

    registros_silver = (
        spark.table(
            f"workspace.silver.{tabela}"
        )
        .count()
    )

    diferenca = (
        registros_silver - registros_bronze
    )

    if diferenca == 0:
        status = "OK"
    else:
        status = "VERIFICAR"

    comparacao_camadas.append({
        "tabela": tabela,
        "registros_bronze": registros_bronze,
        "registros_silver": registros_silver,
        "diferenca": diferenca,
        "status": status
    })

df_comparacao_camadas = spark.createDataFrame(
    comparacao_camadas
)

display(
    df_comparacao_camadas.orderBy("tabela")
)

# COMMAND ----------

# ============================================================
# VALIDACAO DOS TIPOS SILVER
# ============================================================

spark.table(
    "workspace.silver.orders"
).printSchema()

spark.table(
    "workspace.silver.order_items"
).printSchema()

spark.table(
    "workspace.silver.products"
).printSchema()

# COMMAND ----------

# ============================================================
# VALIDAR CAMPOS DERIVADOS
# ============================================================

display(
    spark.table("workspace.silver.orders")
    .select(
        "order_id",
        "order_status",
        "purchase_date",
        "delivery_days",
        "delay_days",
        "is_delayed"
    )
    .limit(20)
)

# COMMAND ----------

display(
    spark.table("workspace.silver.order_items")
    .select(
        "order_id",
        "order_item_id",
        "price",
        "freight_value",
        "item_total_value",
        "freight_percentage"
    )
    .limit(20)
)

# COMMAND ----------

display(
    spark.table("workspace.silver.products")
    .select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
        "product_volume_cm3"
    )
    .limit(20)
)

# COMMAND ----------

# ============================================================
# VALIDACAO FINAL DA CAMADA SILVER
# ============================================================

total_tabelas = len(comparacao_camadas)

tabelas_com_diferenca = [
    item
    for item in comparacao_camadas
    if item["diferenca"] != 0
]

print("=" * 60)
print("TRANSFORMACAO DA CAMADA SILVER CONCLUIDA")
print("=" * 60)
print(f"Tabelas criadas: {total_tabelas}")
print(
    f"Tabelas com diferenca de registros: "
    f"{len(tabelas_com_diferenca)}"
)
print("Conversao de tipos: CONCLUIDA")
print("Padronizacao de textos: CONCLUIDA")
print("Enriquecimento de produtos: CONCLUIDO")
print("Campos derivados: CRIADOS")
print("Bronze alterada: NAO")

if not tabelas_com_diferenca:
    print("Status geral: OK")
else:
    print("Status geral: VERIFICAR")

print("=" * 60)