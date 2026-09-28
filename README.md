# MVP de Engenharia de Dados: Pipeline Lakehouse Olist

## Visão Geral

Este projeto apresenta a construção de um pipeline de dados de ponta a
ponta em ambiente de nuvem utilizando Databricks, PySpark, Spark SQL,
Delta Lake, Unity Catalog e arquitetura medalhão.

O pipeline processa dados públicos de um e-commerce brasileiro,
organizando as informações nas camadas Bronze, Silver e Gold.

Ao final, os dados são utilizados para analisar vendas, entregas,
avaliações de clientes, categorias de produtos e custos de frete.

---

## 1. Contexto de Negócio e Perguntas

O comércio eletrônico depende tanto do desempenho comercial quanto da
eficiência logística.

Atrasos nas entregas, custos elevados de frete, concentração de vendas
em determinadas categorias e avaliações negativas podem afetar a
experiência dos clientes e os resultados da operação.

### Problema de negócio

Como os aspectos comerciais e logísticos dos pedidos influenciam o
desempenho das vendas e a experiência dos clientes do e-commerce?

### Perguntas de negócio

1. Como o valor vendido e a quantidade de pedidos evoluem ao longo dos meses?
2. Quais categorias de produtos geram maior faturamento e quantidade de vendas?
3. Quais estados concentram o maior valor vendido e a maior quantidade de pedidos?
4. Qual é a proporção de pedidos entregues com atraso?
5. Pedidos atrasados recebem avaliações inferiores aos pedidos entregues dentro do prazo?
6. Quais categorias apresentam maior participação do frete no valor total da compra?

---

## 2. Fonte, Contexto e Licença dos Dados

Os dados utilizados neste projeto foram obtidos no dataset
**Brazilian E-Commerce Public Dataset by Olist**, disponibilizado na
plataforma Kaggle.

A base contém 
