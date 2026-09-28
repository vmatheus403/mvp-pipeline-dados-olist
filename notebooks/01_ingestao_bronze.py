# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingestão da Camada Bronze
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Realizar a ingestão dos arquivos CSV originais do dataset Olist,
# MAGIC armazenados no Volume do Unity Catalog, para tabelas Delta no schema
# MAGIC `workspace.bronze`.
# MAGIC
# MAGIC Nesta camada, os dados são preservados próximos ao formato original.
# MAGIC Não são realizadas transformações de negócio, exclusões, deduplicações
# MAGIC ou tratamentos de valores nulos.
# MAGIC
# MAGIC São adicionados somente metadados técnicos para garantir a
# MAGIC rastreabilidade da ingestão:
# MAGIC
# MAGIC - nome do arquivo de origem;
# MAGIC - data e hora da ingestão;
# MAGIC - data da ingestão.
# MAGIC
# MAGIC ## Origem
# MAGIC
# MAGIC `/Volumes/workspace/bronze/olist_arquivos`
# MAGIC
# MAGIC ## Destino
# MAGIC
# MAGIC `workspace.bronze`

# COMMAND ----------

# ============================================================
# CONFIGURAÇÕES DA INGESTÃO BRONZE
# ============================================================

from pyspark.sql import functions as F

CATALOGO = "workspace"
SCHEMA_BRONZE = "bronze"

CAMINHO_VOLUME = (
    "/Volumes/workspace/bronze/olist_arquivos"
)

ARQUIVOS_TABELAS = {
    "olist_customers_dataset.csv": "customers",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "category_translation"
}

print("=" * 60)
print("CONFIGURAÇÃO DA INGESTÃO BRONZE")
print("=" * 60)
print(f"Origem: {CAMINHO_VOLUME}")
print(f"Destino: {CATALOGO}.{SCHEMA_BRONZE}")
print(f"Quantidade de arquivos: {len(ARQUIVOS_TABELAS)}")
print("=" * 60)

# COMMAND ----------

# ============================================================
# TESTE DE LEITURA DO ARQUIVO DE CLIENTES
# ============================================================

caminho_customers = (
    f"{CAMINHO_VOLUME}/olist_customers_dataset.csv"
)

df_customers_teste = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .load(caminho_customers)
)

print("Arquivo de clientes lido com sucesso.")
print(f"Quantidade de colunas: {len(df_customers_teste.columns)}")

display(df_customers_teste.limit(10))

# COMMAND ----------

# Visualizar o esquema técnico lido na Bronze

df_customers_teste.printSchema()

# COMMAND ----------

# ============================================================
# INGESTÃO DOS ARQUIVOS CSV PARA TABELAS DELTA BRONZE
# ============================================================

resultados_ingestao = []

for nome_arquivo, nome_tabela in ARQUIVOS_TABELAS.items():

    caminho_arquivo = f"{CAMINHO_VOLUME}/{nome_arquivo}"

    tabela_destino = (
        f"{CATALOGO}.{SCHEMA_BRONZE}.{nome_tabela}"
    )

    print("-" * 60)
    print(f"Lendo arquivo: {nome_arquivo}")
    print(f"Tabela de destino: {tabela_destino}")

    df_bronze = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .load(caminho_arquivo)
    )

    # Inclusão apenas de metadados técnicos
    df_bronze = (
        df_bronze
        .withColumn(
            "_source_file",
            F.lit(nome_arquivo)
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_ingestion_date",
            F.current_date()
        )
    )

    quantidade_registros = df_bronze.count()
    quantidade_colunas = len(df_bronze.columns)

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_destino)
    )

    resultados_ingestao.append(
        {
            "arquivo": nome_arquivo,
            "tabela": tabela_destino,
            "registros": quantidade_registros,
            "colunas": quantidade_colunas,
            "status": "OK"
        }
    )

    print(f"Registros carregados: {quantidade_registros}")
    print(f"Colunas gravadas: {quantidade_colunas}")
    print("Status: OK")

print("-" * 60)
print("INGESTÃO BRONZE CONCLUÍDA")
print("-" * 60)


# COMMAND ----------

# ============================================================
# RESUMO DA INGESTÃO
# ============================================================

df_resultados = spark.createDataFrame(resultados_ingestao)

display(
    df_resultados.orderBy("tabela")
)


# COMMAND ----------

# Listar as tabelas Delta criadas na camada Bronze

display(
    spark.sql(
        "SHOW TABLES IN workspace.bronze"
    )
)

# COMMAND ----------

# ============================================================
# VALIDAR AS TABELAS DELTA PERSISTIDAS
# ============================================================

validacao_tabelas = []

for nome_tabela in ARQUIVOS_TABELAS.values():

    tabela_completa = (
        f"{CATALOGO}.{SCHEMA_BRONZE}.{nome_tabela}"
    )

    df_tabela = spark.table(tabela_completa)

    validacao_tabelas.append(
        {
            "tabela": tabela_completa,
            "registros_delta": df_tabela.count(),
            "colunas_delta": len(df_tabela.columns),
            "status_leitura": "OK"
        }
    )

df_validacao_tabelas = spark.createDataFrame(
    validacao_tabelas
)

display(
    df_validacao_tabelas.orderBy("tabela")
)

# COMMAND ----------

# Confirmar metadados técnicos da tabela de pedidos

display(
    spark.table("workspace.bronze.orders")
    .select(
        "_source_file",
        "_ingestion_timestamp",
        "_ingestion_date"
    )
    .limit(10)
)

# COMMAND ----------

# ============================================================
# RESUMO FINAL DA INGESTÃO BRONZE
# ============================================================

quantidade_tabelas = len(validacao_tabelas)

print("=" * 60)
print("INGESTÃO DA CAMADA BRONZE CONCLUÍDA")
print("=" * 60)
print(f"Arquivos processados: {len(ARQUIVOS_TABELAS)}")
print(f"Tabelas Delta validadas: {quantidade_tabelas}")
print("Tabelas com erro: 0")
print("Destino: workspace.bronze")
print("Status geral: OK")
print("=" * 60)