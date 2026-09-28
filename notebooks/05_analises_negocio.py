# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análises de Negócio
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Utilizar as tabelas dimensionais da camada Gold para responder às
# MAGIC perguntas de negócio definidas no início do MVP.
# MAGIC
# MAGIC ## Perguntas analisadas
# MAGIC
# MAGIC 1. Como o valor vendido e a quantidade de pedidos evoluem ao longo dos meses?
# MAGIC 2. Quais categorias geram maior faturamento e maior quantidade de vendas?
# MAGIC 3. Quais estados concentram o maior valor de vendas e a maior quantidade de pedidos?
# MAGIC 4. Qual é a proporção de pedidos entregues com atraso?
# MAGIC 5. Pedidos atrasados recebem avaliações inferiores aos pedidos entregues no prazo?
# MAGIC 6. Quais categorias apresentam maior participação do frete no valor da compra?
# MAGIC
# MAGIC ## Fonte
# MAGIC
# MAGIC Tabelas Delta disponíveis no schema `workspace.gold`.

# COMMAND ----------

# ============================================================
# CONFIGURACAO DAS ANALISES
# ============================================================

from pyspark.sql import functions as F

df_fato_pedidos = spark.table(
    "workspace.gold.fato_pedidos"
)

df_fato_itens = spark.table(
    "workspace.gold.fato_itens_pedido"
)

df_dim_cliente = spark.table(
    "workspace.gold.dim_cliente"
)

df_dim_produto = spark.table(
    "workspace.gold.dim_produto"
)

df_dim_vendedor = spark.table(
    "workspace.gold.dim_vendedor"
)

df_dim_tempo = spark.table(
    "workspace.gold.dim_tempo"
)

print("=" * 60)
print("TABELAS GOLD CARREGADAS")
print("=" * 60)
print("fato_pedidos: OK")
print("fato_itens_pedido: OK")
print("dim_cliente: OK")
print("dim_produto: OK")
print("dim_vendedor: OK")
print("dim_tempo: OK")
print("=" * 60)

# COMMAND ----------

# ============================================================
# PERIODO ANALISADO
# ============================================================

periodo = (
    df_fato_pedidos
    .agg(
        F.min("purchase_date").alias("data_inicial"),
        F.max("purchase_date").alias("data_final"),
        F.countDistinct("order_id").alias(
            "quantidade_pedidos"
        )
    )
)

display(periodo)

# COMMAND ----------

# PERGUNTA 1 - EVOLUCAO MENSAL

analise_mensal = (
    df_fato_pedidos
    .filter(F.col("purchase_date").isNotNull())
    .withColumn(
        "ano_mes",
        F.date_format("purchase_date", "yyyy-MM")
    )
    .groupBy("ano_mes")
    .agg(
        F.countDistinct("order_id").alias("quantidade_pedidos"),
        F.round(
            F.sum("product_total_value"),
            2
        ).alias("valor_produtos"),
        F.round(
            F.sum("freight_total_value"),
            2
        ).alias("valor_frete"),
        F.round(
            F.sum("order_items_total_value"),
            2
        ).alias("valor_total_com_frete")
    )
    .orderBy("ano_mes")
)

display(analise_mensal)

# COMMAND ----------

maior_mes_pedidos = (
    analise_mensal
    .orderBy(F.desc("quantidade_pedidos"))
    .first()
)

maior_mes_valor = (
    analise_mensal
    .orderBy(F.desc("valor_produtos"))
    .first()
)

print("PERGUNTA 1 - RESULTADO")
print(
    "Maior mes em pedidos:",
    maior_mes_pedidos["ano_mes"],
    maior_mes_pedidos["quantidade_pedidos"]
)
print(
    "Maior mes em valor:",
    maior_mes_valor["ano_mes"],
    maior_mes_valor["valor_produtos"]
)

# COMMAND ----------

# PERGUNTA 2 - CATEGORIAS COM MAIOR FATURAMENTO

analise_categorias = (
    df_fato_itens
    .join(
        df_dim_produto.select(
            "product_id",
            "product_category_name_english"
        ),
        on="product_id",
        how="left"
    )
    .withColumn(
        "categoria",
        F.coalesce(
            F.col("product_category_name_english"),
            F.lit("nao_informado")
        )
    )
    .groupBy("categoria")
    .agg(
        F.countDistinct("order_id").alias("quantidade_pedidos"),
        F.count(F.lit(1)).alias("quantidade_itens"),
        F.round(
            F.sum("price"),
            2
        ).alias("valor_produtos"),
        F.round(
            F.sum("freight_value"),
            2
        ).alias("valor_frete")
    )
    .orderBy(F.desc("valor_produtos"))
)

display(
    analise_categorias.limit(10)
)

# COMMAND ----------

# PERGUNTA 3 - VENDAS POR ESTADO DO CLIENTE

analise_estados = (
    df_fato_pedidos
    .join(
        df_dim_cliente.select(
            "customer_id",
            "customer_state"
        ),
        on="customer_id",
        how="left"
    )
    .groupBy("customer_state")
    .agg(
        F.countDistinct("order_id").alias("quantidade_pedidos"),
        F.round(
            F.sum("product_total_value"),
            2
        ).alias("valor_produtos"),
        F.round(
            F.sum("freight_total_value"),
            2
        ).alias("valor_frete"),
        F.round(
            F.avg("product_total_value"),
            2
        ).alias("valor_medio_por_pedido")
    )
    .orderBy(F.desc("valor_produtos"))
)

display(
    analise_estados.limit(10)
)

# COMMAND ----------

# PERGUNTA 4 - PROPORCAO DE PEDIDOS ATRASADOS

pedidos_com_entrega = (
    df_fato_pedidos
    .filter(
        F.col("order_delivered_customer_date").isNotNull()
    )
    .filter(
        F.col("order_estimated_delivery_date").isNotNull()
    )
)

analise_atrasos = (
    pedidos_com_entrega
    .groupBy("is_delayed")
    .agg(
        F.countDistinct("order_id").alias("quantidade_pedidos")
    )
    .withColumn(
        "situacao_entrega",
        F.when(
            F.col("is_delayed") == True,
            F.lit("Atrasado")
        ).otherwise(
            F.lit("Dentro do prazo")
        )
    )
)

total_pedidos_entregues = (
    pedidos_com_entrega
    .select("order_id")
    .distinct()
    .count()
)

analise_atrasos = (
    analise_atrasos
    .withColumn(
        "percentual",
        F.round(
            F.col("quantidade_pedidos")
            * 100.0
            / F.lit(total_pedidos_entregues),
            2
        )
    )
    .select(
        "situacao_entrega",
        "quantidade_pedidos",
        "percentual"
    )
    .orderBy(F.desc("quantidade_pedidos"))
)

display(analise_atrasos)

# COMMAND ----------

# PERGUNTA 5 - ATRASO E AVALIACAO

analise_atraso_avaliacao = (
    df_fato_pedidos
    .filter(F.col("is_delayed").isNotNull())
    .filter(F.col("average_review_score").isNotNull())
    .withColumn(
        "situacao_entrega",
        F.when(
            F.col("is_delayed") == True,
            F.lit("Atrasado")
        ).otherwise(
            F.lit("Dentro do prazo")
        )
    )
    .groupBy("situacao_entrega")
    .agg(
        F.countDistinct("order_id").alias("quantidade_pedidos"),
        F.round(
            F.avg("average_review_score"),
            2
        ).alias("avaliacao_media"),
        F.round(
            F.avg("delay_days"),
            2
        ).alias("media_dias_atraso")
    )
    .orderBy("situacao_entrega")
)

display(analise_atraso_avaliacao)

# COMMAND ----------

# PERGUNTA 6 - PARTICIPACAO DO FRETE POR CATEGORIA

analise_frete_categoria = (
    df_fato_itens
    .join(
        df_dim_produto.select(
            "product_id",
            "product_category_name_english"
        ),
        on="product_id",
        how="left"
    )
    .withColumn(
        "categoria",
        F.coalesce(
            F.col("product_category_name_english"),
            F.lit("nao_informado")
        )
    )
    .groupBy("categoria")
    .agg(
        F.count(F.lit(1)).alias("quantidade_itens"),
        F.round(
            F.sum("price"),
            2
        ).alias("valor_produtos"),
        F.round(
            F.sum("freight_value"),
            2
        ).alias("valor_frete"),
        F.round(
            F.sum("item_total_value"),
            2
        ).alias("valor_total")
    )
    .withColumn(
        "percentual_frete",
        F.round(
            F.col("valor_frete")
            * 100.0
            / F.col("valor_total"),
            2
        )
    )
    .filter(F.col("quantidade_itens") >= 100)
    .orderBy(F.desc("percentual_frete"))
)

display(
    analise_frete_categoria.limit(10)
)

# COMMAND ----------

# ============================================================
# ENCERRAMENTO DAS ANALISES
# ============================================================

print("=" * 60)
print("ANALISES DE NEGOCIO CONCLUIDAS")
print("=" * 60)
print("Pergunta 1 - Evolucao mensal: CONCLUIDA")
print("Pergunta 2 - Categorias: CONCLUIDA")
print("Pergunta 3 - Estados: CONCLUIDA")
print("Pergunta 4 - Atrasos: CONCLUIDA")
print("Pergunta 5 - Atraso e avaliacao: CONCLUIDA")
print("Pergunta 6 - Frete por categoria: CONCLUIDA")
print("Perguntas respondidas: 6")
print("Status geral: OK")
print("=" * 60)

# COMMAND ----------

# ============================================================
# CATALOGO TECNICO DAS TABELAS GOLD
# ============================================================

tabelas_gold = [
    "dim_cliente",
    "dim_produto",
    "dim_vendedor",
    "dim_tempo",
    "fato_pedidos",
    "fato_itens_pedido"
]

catalogo_tecnico = []

for tabela in tabelas_gold:

    df_tabela = spark.table(
        f"workspace.gold.{tabela}"
    )

    for campo in df_tabela.schema.fields:

        catalogo_tecnico.append({
            "tabela": tabela,
            "coluna": campo.name,
            "tipo_dado": campo.dataType.simpleString(),
            "permite_nulo": campo.nullable
        })

df_catalogo_tecnico = spark.createDataFrame(
    catalogo_tecnico
)

display(
    df_catalogo_tecnico.orderBy(
        "tabela",
        "coluna"
    )
)