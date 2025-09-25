![logo_ironhack_blue 7](https://user-images.githubusercontent.com/23629340/40541063-a07a0a8a-601a-11e8-91b5-2f13e4e6b441.png)

# Lab Avanzado | ETL con Transformaciones Complejas y Modelado en Snowflake

## 🎯 Objetivo

Aplicar un flujo ETL complejo sobre datos de pedidos, incluyendo validaciones avanzadas, normalización parcial, enriquecimiento de datos y estandarización con nomenclatura corporativa SEAT.

## Requisitos

* Haz un ***fork*** de este repositorio.
* Clona este repositorio.

## Entrega

- Haz Commit y Push
- Crea un Pull Request (PR)
- Copia el enlace a tu PR (con tu solución) y pégalo en el campo de entrega del portal del estudiante – solo así se considerará entregado el lab

## 📁 Dataset

**Archivo de origen**: `B_ORDERS_RAW_COMPLEX.csv` (1000 filas)  
Contiene errores realistas: fechas inválidas, nulos, valores erróneos, campos de texto libre y datos semiestructurados.

### Columnas originales

| Columna | Descripción | Posibles errores o valores |
| --- | --- | --- |
| ID_ORDER | Identificador del pedido | -   |
| DTE_ORDER | Fecha del pedido | BAD_DATE |
| ID_CUSTOMER | Identificador del cliente | NULL |
| ID_PART | ID del recambio solicitado | "INVALID" |
| QTY_ORDERED | Cantidad pedida | "X", "null", "-5" |
| AMT_TOTAL | Importe total del pedido | "ERROR", NULL |
| REF_PAYMENT_METHOD | Método de pago (referencia) | NULL, valores no estándar |
| DTE_DELIVERY_EST | Fecha estimada de entrega | NULL |
| DES_ORDER_NOTE | Nota libre escrita por el cliente | NULL |

## 🧱 BRONZE – Carga de datos sin modificar

```sql
CREATE OR REPLACE TABLE DEV_BRONZE_DB.B_LEGACY_ERP.B_ORDERS_RAW_COMPLEX (
  ID_ORDER STRING,
  DTE_ORDER STRING,
  ID_CUSTOMER STRING,
  ID_PART STRING,
  QTY_ORDERED STRING,
  AMT_TOTAL STRING,
  REF_PAYMENT_METHOD STRING,
  DTE_DELIVERY_EST STRING,
  DES_ORDER_NOTE STRING
);
```

## 🧼 SILVER – Limpieza, validación y enriquecimiento

### 1. Crear tabla de referencia de métodos de pago

```sql
CREATE OR REPLACE TABLE DEV_SILVER_DB.S_SUPPLY_CHAIN.S_PAYMENT_METHODS (
  REF_PAYMENT_METHOD STRING PRIMARY KEY,
  DES_PAYMENT_METHOD VARCHAR
);

INSERT INTO DEV_SILVER_DB.S_SUPPLY_CHAIN.S_PAYMENT_METHODS VALUES
  ('CARD', 'Credit or debit card'),
  ('CASH', 'Cash at delivery'),
  ('TRANSFER', 'Bank transfer'),
  ('CRYPTO', 'Cryptocurrency');
```

### 2. Transformar y limpiar datos del raw

```sql
CREATE OR REPLACE TABLE DEV_SILVER_DB.S_SUPPLY_CHAIN.S_ORDERS_CLEAN_COMPLEX AS
SELECT
  ID_ORDER AS ID_ORDER,
  TRY_TO_DATE(DTE_ORDER, 'YYYY-MM-DD') AS DTE_ORDER,
  ID_CUSTOMER AS ID_CUSTOMER,
  ID_PART AS ID_PART,
  CASE 
    WHEN IS_NUMBER(QTY_ORDERED) AND TRY_TO_NUMBER(QTY_ORDERED) > 0 THEN TRY_TO_NUMBER(QTY_ORDERED)
    ELSE NULL 
  END AS QTY_ORDERED,
  TRY_TO_NUMBER(AMT_TOTAL) AS AMT_TOTAL,
  UPPER(REF_PAYMENT_METHOD) AS REF_PAYMENT_METHOD,
  TRY_TO_DATE(DTE_DELIVERY_EST, 'YYYY-MM-DD') AS DTE_DELIVERY_EST,
  DES_ORDER_NOTE AS DES_ORDER_NOTE,
  CURRENT_TIMESTAMP() AS AUD_TST_LOAD,
  CURRENT_USER() AS AUD_USR_LOAD
FROM DEV_BRONZE_DB.B_LEGACY_ERP.B_ORDERS_RAW_COMPLEX
WHERE
  TRY_TO_DATE(DTE_ORDER, 'YYYY-MM-DD') IS NOT NULL
  AND ID_CUSTOMER IS NOT NULL
  AND ID_PART NOT IN ('INVALID')
  AND TRY_TO_NUMBER(AMT_TOTAL) IS NOT NULL;
```

## 📊 GOLD – Agregaciones y vistas analíticas

### Crear vista: ventas por método de pago

```sql
CREATE OR REPLACE VIEW DEV_GOLD_DB.G_SALES.G_ORDERS_BY_PAYMENT_METHOD AS
SELECT
  REF.REF_PAYMENT_METHOD,
  PAY.DES_PAYMENT_METHOD,
  COUNT(*) AS NUM_ORDERS,
  SUM(QTY_ORDERED) AS TOTAL_QTY,
  SUM(AMT_TOTAL) AS TOTAL_REVENUE
FROM DEV_SILVER_DB.S_SUPPLY_CHAIN.S_ORDERS_CLEAN_COMPLEX REF
LEFT JOIN DEV_SILVER_DB.S_SUPPLY_CHAIN.S_PAYMENT_METHODS PAY
ON REF.REF_PAYMENT_METHOD = PAY.REF_PAYMENT_METHOD
GROUP BY REF.REF_PAYMENT_METHOD, PAY.DES_PAYMENT_METHOD;
```

## 🧠 Preguntas de análisis

1. ¿Cuántos registros fueron descartados por errores?
   cargan todos
3. ¿Qué proporción de métodos de pago están ausentes o mal escritos?
4. ¿Cuál es el ticket medio por pedido?
5. ¿Cuántos pedidos no tienen fecha de entrega informada?

## 📌 Buenas prácticas aplicadas

- Separación de responsabilidades por capas (Bronze/Silver/Gold).
- Aplicación de nomenclatura corporativa (prefijos, campos `AUD_`, etc.).
- Normalización parcial mediante tabla de métodos de pago.
- Validación robusta con funciones `TRY_TO_`, `CASE`, `IS_NUMBER`.
- Uso de vistas para consumo analítico (Gold).
- Preparación para explotación vía BI o visualización.

Aquí tienes la sección **📦 Entregables** y **🧾 Entrega** para el *Lab Avanzado | ETL con Transformaciones Complejas y Modelado en Snowflake*, lista para copiar y pegar directamente al final de tu lab:

## 📦 Entregables

Dentro de tu repositorio forkeado, asegúrate de incluir los siguientes archivos:

* `bronze.sql` – Script con la creación de la tabla Bronze (`B_ORDERS_RAW_COMPLEX`) y carga inicial del CSV
* `silver.sql` – Script con:

  * La creación de la tabla de métodos de pago (`S_PAYMENT_METHODS`)
  * La transformación y limpieza (`S_ORDERS_CLEAN_COMPLEX`)
* `gold.sql` – Script con la creación de la vista `G_ORDERS_BY_PAYMENT_METHOD`
* `lab-notes.md` – Documento explicativo que incluya:

  * Cuántos registros fueron descartados 371
  * Qué errores o inconsistencias predominaban: inconsistencias de datos, nulos, fechas como string
  * Qué validaciones aplicaste (formato, tipos, nulos, valores inválidos)
 Validaciones que aplica tu CTAS (y qué protegen)

Formato y tipos

TRY_TO_DATE(DTE_ORDER, 'YYYY-MM-DD') y TRY_TO_DATE(DTE_DELIVERY_EST, 'YYYY-MM-DD'): valida formato ISO; si falla, devuelve NULL (y además filtras DTE_ORDER IS NOT NULL).

TRY_TO_NUMBER(AMT_TOTAL): asegura numérico en importe total (filtras IS NOT NULL).

TRY_TO_NUMBER(QTY_ORDERED) dentro de CASE: garantiza numérico y mayor que 0; si no, lo anulas.

UPPER(REF_PAYMENT_METHOD): normaliza el dominio de métodos de pago (evita duplicidades por casing).

Nulos

WHERE ... TRY_TO_DATE(DTE_ORDER)... IS NOT NULL: obligas fecha de pedido válida.

AND ID_CUSTOMER IS NOT NULL: evitas registros huérfanos de cliente.

AND TRY_TO_NUMBER(AMT_TOTAL) IS NOT NULL: evitas importes no parseables.

Valores inválidos / dominios

AND ID_PART NOT IN ('INVALID'): bloqueo explícito de un valor conocido como inválido.

QTY_ORDERED > 0: evita cantidades cero/negativas.

Auditoría

CURRENT_TIMESTAMP() y CURRENT_USER(): traqueo de carga
  * Propuesta de otras vistas analíticas en Gold

CREATE OR REPLACE VIEW G_V_FACT_ORDERS_DAILY AS
SELECT
  DATE_TRUNC('day', DTE_ORDER) AS day,
  COUNT(DISTINCT ID_ORDER)     AS orders,
  SUM(QTY_ORDERED)             AS items,
  SUM(AMT_TOTAL)               AS revenue,
  AVG(AMT_TOTAL)               AS avg_ticket
FROM S_ORDERS_CLEAN_COMPLEX
GROUP BY 1;

CREATE OR REPLACE VIEW G_V_PAYMENT_MIX AS
SELECT
  DATE_TRUNC('month', DTE_ORDER) AS month,
  REF_PAYMENT_METHOD,
  COUNT(DISTINCT ID_ORDER)        AS orders,
  SUM(AMT_TOTAL)                  AS revenue,
  100.0 * SUM(AMT_TOTAL) / NULLIF(SUM(SUM(AMT_TOTAL)) OVER (PARTITION BY DATE_TRUNC('month', DTE_ORDER)),0) AS revenue_share_pct,
  AVG(AMT_TOTAL)                  AS avg_ticket
FROM S_ORDERS_CLEAN_COMPLEX
GROUP BY 1,2;
  * Respuestas a las preguntas del apartado **🧠 Preguntas de análisis**
* *(Opcional)* Capturas de pantalla de las vistas o consultas ejecutadas en Snowflake

## ✅ Conclusión

Has aplicado un flujo ETL robusto sobre un dataset semi-estructurado, corrigiendo errores, integrando referencias, aplicando auditoría y generando vistas analíticas. Este ejercicio refleja el tipo de proceso que se encuentra en entornos reales en SEAT u otras grandes compañías de datos.

- 📁 Dataset utilizado: `B_ORDERS_RAW_COMPLEX.csv`
