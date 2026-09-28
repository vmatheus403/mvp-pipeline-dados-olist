# Databricks notebook source
# Teste inicial do ambiente Databricks

print("Ambiente Databricks iniciado com sucesso!")

df_teste = spark.range(1, 6)

display(df_teste)

# COMMAND ----------

# Identificação do catálogo e do schema atuais

catalogo_atual = spark.sql("SELECT current_catalog() AS catalogo")
schema_atual = spark.sql("SELECT current_schema() AS schema")

print("Catálogo atual:")
display(catalogo_atual)

print("Schema atual:")
display(schema_atual)

# COMMAND ----------



# COMMAND ----------

# Lista dos catálogos disponíveis no ambiente

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# ============================================================
# CONFIGURAÇÃO INICIAL DO PROJETO
# MVP Pipeline de Dados Olist
# ============================================================

CATALOGO = spark.sql(
    "SELECT current_catalog() AS catalogo"
).first()["catalogo"]

SCHEMAS = ["bronze", "silver", "gold"]

print(f"Catálogo selecionado: {CATALOGO}")

for schema in SCHEMAS:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{CATALOGO}`.`{schema}`"
    )
    print(f"Schema criado ou já existente: {CATALOGO}.{schema}")

# COMMAND ----------

# Validar os schemas criados

display(
    spark.sql(f"SHOW SCHEMAS IN `{CATALOGO}`")
)

# COMMAND ----------

# Criar uma pequena tabela Delta para validar escrita e leitura

from pyspark.sql import functions as F

df_validacao = (
    spark.range(1, 6)
    .withColumn(
        "descricao",
        F.concat(F.lit("registro_"), F.col("id"))
    )
    .withColumn(
        "data_processamento",
        F.current_timestamp()
    )
)

(
    df_validacao.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"`{CATALOGO}`.`bronze`.`teste_configuracao`"
    )
)

print("Tabela Delta criada com sucesso!")

display(
    spark.table(
        f"`{CATALOGO}`.`bronze`.`teste_configuracao`"
    )
)

# COMMAND ----------

# Listar as tabelas existentes na camada Bronze

display(
    spark.sql(
        f"SHOW TABLES IN `{CATALOGO}`.`bronze`"
    )
)

# COMMAND ----------

# Remover a tabela utilizada somente para teste

spark.sql(
    f"DROP TABLE IF EXISTS "
    f"`{CATALOGO}`.`bronze`.`teste_configuracao`"
)

print("Tabela técnica removida com sucesso!")

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN `{CATALOGO}`.`bronze`"
    )
)

# COMMAND ----------

# Resumo da configuração do ambiente

print("=" * 60)
print("CONFIGURAÇÃO INICIAL CONCLUÍDA")
print("=" * 60)
print(f"Catálogo do projeto: {CATALOGO}")
print(f"Camada Bronze: {CATALOGO}.bronze")
print(f"Camada Silver: {CATALOGO}.silver")
print(f"Camada Gold: {CATALOGO}.gold")
print("Execução Spark: OK")
print("Criação de schemas: OK")
print("Escrita e leitura de tabela Delta: OK")
print("=" * 60)

# COMMAND ----------

# Criar o Volume para armazenar os arquivos CSV originais

spark.sql("""
    CREATE VOLUME IF NOT EXISTS workspace.bronze.olist_arquivos
    COMMENT 'Arquivos CSV originais do dataset Olist'
""")

print("Volume criado ou já existente:")
print("/Volumes/workspace/bronze/olist_arquivos")

# COMMAND ----------

# Validar o Volume criado

display(
    spark.sql("SHOW VOLUMES IN workspace.bronze")
)

# COMMAND ----------

# Listar os arquivos enviados ao Volume

CAMINHO_VOLUME = "/Volumes/workspace/bronze/olist_arquivos"

arquivos_enviados = dbutils.fs.ls(CAMINHO_VOLUME)

display(arquivos_enviados)

# COMMAND ----------

# Validar os arquivos necessários para o MVP

arquivos_esperados = {
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
}

arquivos_encontrados = {
    arquivo.name
    for arquivo in dbutils.fs.ls(CAMINHO_VOLUME)
}

faltantes = arquivos_esperados - arquivos_encontrados
extras = arquivos_encontrados - arquivos_esperados

print(f"Arquivos esperados: {len(arquivos_esperados)}")
print(f"Arquivos encontrados: {len(arquivos_encontrados)}")

if not faltantes:
    print("Todos os arquivos necessários foram enviados: OK")
else:
    print("Arquivos faltantes:")
    for nome in sorted(faltantes):
        print(f"- {nome}")

if extras:
    print("Arquivos adicionais encontrados:")
    for nome in sorted(extras):
        print(f"- {nome}")

# COMMAND ----------

# Validar os arquivos enviados ao Volume

CAMINHO_VOLUME = "/Volumes/workspace/bronze/olist_arquivos"

arquivos_esperados = {
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
}

arquivos_encontrados = {
    arquivo.name
    for arquivo in dbutils.fs.ls(CAMINHO_VOLUME)
}

faltantes = arquivos_esperados - arquivos_encontrados
extras = arquivos_encontrados - arquivos_esperados

print("=" * 60)
print("VALIDAÇÃO DOS ARQUIVOS DO DATASET OLIST")
print("=" * 60)
print(f"Arquivos esperados: {len(arquivos_esperados)}")
print(f"Arquivos encontrados: {len(arquivos_encontrados)}")

if not faltantes:
    print("Todos os arquivos necessários foram enviados: OK")
else:
    print("Arquivos faltantes:")
    for nome in sorted(faltantes):
        print(f"- {nome}")

if extras:
    print("Arquivos adicionais encontrados:")
    for nome in sorted(extras):
        print(f"- {nome}")

print("=" * 60)