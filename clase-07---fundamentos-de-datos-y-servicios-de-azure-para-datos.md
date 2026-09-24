# Clase 07 - Fundamentos de datos y servicios de Azure para Datos 


# 


# PARTE 1 ? Explorar conceptos de datos

# Secci?n 1: Qu? son los datos y c?mo se clasifican

![](/images/Cb1_Image_1.png)

## **1.2 Datos semiestructurados**

Tienen estructura, pero flexible: cada registro puede tener campos distintos o anidados. El formato m?s habitual es JSON.

{

"id_socio": "S-102938",

"nombre": "Laura M?ndez",

"preferencias": ["ecol?gico", "sin gluten"],

"cupones": [

{ "codigo": "FRUTA10", "canjeado": true, "tienda": "TND-042" },

{ "codigo": "LACT5" }

],

"consentimiento_marketing": true

}

Observa que el segundo cup?n no tiene los campos `canjeado` ni `tienda`. En una tabla relacional eso obligar?a a rellenar con NULL; en JSON simplemente no aparecen.

En Grupo Alimenta: perfiles y eventos de la app de fidelizaci?n, pedidos de la tienda online recibidos por API, clics de la web.

## **1.3 Datos no estructurados**

![](/images/EXU_Image_2.png)

## **Ejercicio 1 ? Clasifica las fuentes**

Clasifica cada fuente como estructurada, semiestructurada o no estructurada y justifica tu respuesta:

- **1. Exportaci?n diaria del ERP de compras en CSV:** 
- 
- **2. Lecturas de temperatura de las c?maras frigor?ficas enviadas cada 30 segundos en JSON:** 
- 
- **3. Grabaciones MP3 del servicio de atenci?n al cliente:** 
- 
- **4. Tabla Empleados de la base de datos de RR. HH.:** 
- 
- **5. Logs de acceso de la tienda online:** 
- 
- **6. Fotograf?as de los lineales tomadas por los reponedores:** 
- 
```
Nota:?Qu? es un ERP y en qu? se diferencia de un CRM?

```
# Secci?n 2: Almacenamiento de datos en ficheros

![](/images/5pz_Image_3.png)

![](/images/pbk_Image_4.png)

## **2.4 BLOB (Binary Large Object)**

![](/images/xPc_Image_5.png)

## **2.5 Formatos optimizados: el terreno del ingeniero de datos**

![](/images/FR5_Image_6.png)

## **2.6 Comparativa r?pida**

![](/images/YEM_Image_7.png)

![](/images/SK1_Image_8.png)

![](/images/G8t_Image_9.png)

## **2.7 Demostraci?n: de CSV a Delta en un notebook de Spark**

![](/images/axn_Image_10.png)

## **Ejercicio 2 ? Elige el formato**

Para cada escenario de Grupo Alimenta, elige el formato m?s adecuado y justifica la decisi?n:

1. Un proveedor peque?o nos env?a cada semana su tarifa de precios y solo sabe trabajar con Excel.

2. Almacenar 5 a?os de l?neas de ticket para que los analistas estudien la estacionalidad de ventas por familia de producto.

3. Tabla de socios de fidelizaci?n que se actualiza a diario y sobre la que el delegado de protecci?n de datos exige poder ver c?mo estaba hace un mes.

4. Flujo continuo de lecturas de temperatura de las c?maras frigor?ficas.

5. Recepci?n de pedidos de compra con un gran proveedor que usa EDI.

# Secci?n 3: Almacenamiento de datos en bases de datos

![](/images/cpr_Image_11.png)

![](/images/zr2_Image_12.png)

Las bases relacionales se consultan con SQL (Structured Query Language):SELECT t.ciudad,

COUNT(k.id_ticket) AS num_tickets

FROM   Tiendas t

JOIN   Tickets k ON k.id_tienda = t.id_tienda

WHERE  k.fecha_hora >= '2026-09-01'

GROUP  BY t.ciudad

ORDER  BY num_tickets DESC;

## **3.2 Bases de datos no relacionales (NoSQL)**

![](/images/QRN_Image_13.png)

# Secci?n 4: Procesamiento de datos transaccional (OLTP)

![](/images/MSw_Image_14.png)

BEGIN TRANSACTION;

INSERT INTO PedidosOnline (id_pedido, id_socio, id_tienda, fecha)

VALUES ('PO-77120', 'S-102938', 'TND-042', GETDATE());

UPDATE Inventario

SET    stock = stock - 6

WHERE  id_producto = 501 AND id_tienda = 'TND-042' AND stock >= 6;

IF @@ROWCOUNT = 0

ROLLBACK TRANSACTION;   -- no hay stock: se deshace todo

ELSE

COMMIT TRANSACTION;     -- ambas operaciones quedan confirmadas

![](/images/08L_Image_15.png)

![](/images/h2y_Image_18.png)

# Secci?n 5: Procesamiento de datos anal?tico

![](/images/CTw_Image_19.png)

## **5.2 ETL frente a ELT**

![](/images/tQt_Image_20.png)

## **5.3 Data lake, data warehouse y data lakehouse**

![](/images/f3z_Image_21.png)

## **5.4 Modelo dimensional: hechos y dimensiones**

![](/images/NkM_Image_22.png)

## **5.5 OLTP frente a OLAP**

![](/images/MYK_Image_23.png)

## **5.6 Arquitectura medall?n**

![](/images/gzp_Image_24.png)

Bronze ? Silver: limpieza b?sica con PySpark
from pyspark.sql import functions as F

bronze = spark.read.json("Files/bronze/app_fidelizacion/2026/09/")

silver = (bronze

.dropDuplicates(["id_socio", "id_evento"])

.withColumn("ts_utc", F.to_timestamp("timestamp_evento"))

.withColumn("importe", F.col("importe").cast("decimal(10,2)"))

.filter(F.col("id_socio").isNotNull()))

silver.write.format("delta").mode("append").saveAsTable("silver_eventos_socios")

Silver ? Gold: agregaci?n para negocio
gold = (spark.table("silver_eventos_socios")

.filter(F.col("tipo_evento") == "cupon_canjeado")

.groupBy("codigo_cupon", F.to_date("ts_utc").alias("fecha"))

.agg(F.count("*").alias("canjes"),

F.sum("importe").alias("importe_asociado")))

gold.write.format("delta").mode("overwrite").saveAsTable("gold_rendimiento_cupones")

![](/images/5GU_Image_25.png)

## **Ejercicio 3 ? OLTP u OLAP**

Indica si cada necesidad corresponde a un sistema transaccional o anal?tico:

1. Aplicar un cup?n de descuento en la caja.

2. Calcular la evoluci?n de las ventas de productos ecol?gicos en los ?ltimos 3 a?os.

3. Consultar si queda aceite de oliva en la tienda de Valencia ahora mismo.

4. Detectar qu? tiendas tienen m?s roturas de stock los lunes.

5. Actualizar la direcci?n de entrega de un pedido online en curso.

# PARTE 2 ? Roles y servicios de datos en Azure

# Secci?n 6: Los roles del mundo del dato

![](/images/J5u_Image_26.png)

![](/images/uNc_Image_27.png)

![](/images/BAJ_Image_28.png)

![](/images/Gme_Image_29.png)

## **6.5 Cient?fico de datos (Data Scientist)**

![](/images/ABi_Image_30.png)

## **6.6 Arquitecto de datos (Data Architect)**

![](/images/69o_Image_31.png)

## **6.7 Analytics Engineer**

![](/images/5TE_Image_32.png)

## **Ejercicio 4 ? ?Qu? rol es responsable?**

1. El informe de ventas lleva dos d?as sin actualizarse porque ha fallado la carga nocturna.

2. Hay que restaurar la base de datos de la tienda online tras un borrado accidental.

3. Direcci?n quiere un nuevo gr?fico de margen por familia de producto en el cuadro de mando.

4. Atenci?n al cliente quiere un asistente que responda "?cu?ndo llega mi pedido?".

5. Hay que enmascarar los tel?fonos de los socios antes de que lleguen a la capa anal?tica.

6. Marketing quiere saber qu? socios tienen mayor probabilidad de dejar de comprar en los pr?ximos 3 meses.

7. Se debe decidir si la plataforma se construye en Fabric, en Databricks o combinando ambos.

8. Dos departamentos presentan en el comit? cifras distintas de "venta neta" para el mismo mes.

# Secci?n 7: Modelos de servicio en la nube: IaaS, PaaS y SaaS

![](/images/zqz_Image_33.png)

# Secci?n 8: Servicios de datos de Azure organizados por capa

![](/images/4JH_Image_34.png)

## **8.1 Capa de fuentes: bases de datos operativas**

![](/images/toC_Image_35.png)

## **8.2 Capa de almacenamiento**

![](/images/0Rp_Image_36.png)

## **8.3 Capa de ingesta y orquestaci?n**

![](/images/4Zm_Image_37.png)

## **8.4 Capa de procesamiento y modelado**

![](/images/hqe_Image_38.png)

## **8.5 Capa de consumo**

![](/images/xqP_Image_39.png)

## **8.6 Gobierno transversal**

![](/images/kvv_Image_40.png)

## **8.7 Tabla resumen: servicio, rol y tipo de carga**

![](/images/yt0_Image_41.png)

# Secci?n 9: C?mo elegir: Fabric, Databricks o combinaci?n

![](/images/Eg5_Image_42.png)

## **Ejercicio 5**

Investiga y arma un diagrama de arquitectura de capas (como el de la secci?n 8) con cada una de estas tecnolog?as:

- Databricks (no Azure Databricks)

- Microsoft Fabric. Es decir todo enteramente dentro de Fabric

- AWS

- GCP

- Herramientas Open Source (el m?s importante de todos)

?Y qu? pasa con Snowflake, dbt y DuckDB? ?En qu? casos se utilizan? ?En qu? capas se pueden incluir o con cu?les otras tecnolog?as se puede combinar? Crear al menos 3 diagramas para este caso.**Requisitos:**

- Para cada diagrama debes incluir el logo oficial de cada servicio o tecnolog?a y explicar brevemente cada uno.

- Para cada diagrama investiga casos de ?xito.


# Secci?n 10: Caso pr?ctico ? Plataforma de datos de Grupo Alimenta

**Contexto:** \
Grupo Alimenta quiere tres cosas:

1. Un cuadro de mando de ventas, margen y roturas de stock por tienda y familia de producto.

2. Un modelo de previsi?n de demanda de productos frescos para reducir la merma.

3. Un asistente para atenci?n al cliente que responda sobre el estado de los pedidos online.

## **Fuentes disponibles:**

| Fuente | Tecnolog?a | Tipo de dato | Frecuencia |
|---|---|---|---|
| TPV de tiendas y ERP de compras | Azure SQL Database |  | Cambios continuos |
| App de fidelizaci?n | Azure Cosmos DB (JSON) |  | Eventos continuos |
| Sensores de c?maras frigor?ficas | Dispositivos IoT |  | Cada 30 segundos |
| Tarifas de proveedores | CSV por SFTP |  | Semanal |
| Facturas de proveedores | PDF escaneados |  | Por factura |
| Meteorolog?a y festivos | API p?blica |  | Diario |

## **Tareas:**

1. **Clasificaci?n:** Clasifica cada fuente (estructurada, semiestructurada, no estructurada) e indica si es OLTP, fichero o stream.

2. **Arquitectura:** Dibuja una arquitectura con capas Bronze, Silver y Gold. Indica qu? servicio de Azure usar?as en cada paso y justifica si optas por Fabric, Databricks u otro stack.

3. **Formatos:** Indica el formato de almacenamiento de cada capa y por qu?.

4. **Modelo Gold:** Dise?a el esquema en estrella de ventas: tabla de hechos, granularidad, medidas y al menos cuatro dimensiones.

5. **Roles:** Asigna cada paso de la arquitectura a uno de los siete roles vistos, incluyendo arquitecto de datos, analytics engineer y cient?fico de datos.

6. **M?tricas:** Como analytics engineer, redacta la definici?n oficial de "rotura de stock" y dos pruebas de calidad que aplicar?as a la tabla Gold.

7. **Ciencia de datos:** Indica qu? tablas y variables necesitar?a el cient?fico de datos para el modelo de previsi?n de demanda y c?mo escribir?as sus predicciones de vuelta en la plataforma.

8. **Gobierno:** Los datos de socios incluyen nombre, tel?fono y correo. Explica qu? har?as en cada capa y qu? papel juega Purview.

9. **IA:** Explica qu? datos necesitar?a el ingeniero de IA para construir el asistente en Foundry y qu? requisitos de calidad le exigir?as como ingeniero de datos.

# Secci?n 12: Repaso de conceptos

1. Un fichero donde cada evento puede tener campos distintos y anidados es un ejemplo de dato:

- a) Estructurado

- b) Semiestructurado

- c) No estructurado

- d) Binario

- ?Qu? formato almacena juntos los valores de cada columna y es el est?ndar de facto de los lakehouses?

- a) Avro

- b) CSV

- c) Parquet

- d) XML

- ?Qu? aporta Delta Lake sobre Parquet?

- a) Legibilidad humana

- b) Un registro de transacciones con ACID y versionado

- c) Almacenamiento orientado a filas

- d) Soporte exclusivo para im?genes

- Si una transacci?n crea un pedido pero falla la reserva de stock y se deshace todo, se est? garantizando la:

- a) Durabilidad

- b) Atomicidad

- c) Consistencia

- d) Disponibilidad

- En el patr?n ELT, las transformaciones se realizan:

- a) Antes de extraer

- b) En un servidor intermedio

- c) En el sistema de destino

- d) En la aplicaci?n de origen

- ?Qu? capa de la arquitectura medall?n contiene datos limpios, deduplicados y con tipos estandarizados?

- a) Bronze

- b) Silver

- c) Gold

- d) Platinum

- ?Qu? rol es el responsable de monitorizar que los pipelines de carga se ejecutan correctamente?

- a) DBA

- b) Analista de datos

- c) Ingeniero de datos

- d) Ingeniero de IA

- ?Qu? caracter?stica de Azure Storage permite usarlo como data lake?

- a) File shares

- b) Tables

- c) Espacio de nombres jer?rquico sobre Blob

- d) Colas

- ?Cu?l es la opci?n recomendada de orquestaci?n cuando todo el trabajo de datos se realiza dentro de Microsoft Fabric?

- a) Azure Data Factory

- b) Fabric Data Factory

- c) Azure Stream Analytics

- d) Azure Data Explorer

- ?Qu? servicio usar?as para trazar el linaje de un dato desde el TPV hasta un informe de Power BI?

- a) Microsoft Foundry

- b) Azure Cosmos DB

- c) Microsoft Purview

- d) Azure Databricks

- Microsoft Fabric se ofrece como:

- a) IaaS

- b) PaaS

- c) SaaS

- d) On-premises

- Una empresa necesita detectar en tiempo real c?maras frigor?ficas que superan un umbral de temperatura. ?Qu? servicio encaja mejor?

- a) Power BI

- b) Azure Stream Analytics

- c) Azure SQL Managed Instance

- d) Microsoft Purview

- ?Qu? rol es responsable de que la m?trica "venta neta" tenga una ?nica definici?n oficial, probada y documentada?

- a) DBA

- b) Analytics engineer

- c) Ingeniero de IA

- d) Cient?fico de datos

- ?Qu? rol construir?a un modelo para predecir qu? clientes dejar?n de comprar en los pr?ximos meses?

- a) Analista de datos

- b) Arquitecto de datos

- c) Cient?fico de datos

- d) DBA

- ?Qu? rol decide si la plataforma de la empresa se organiza por dominios y qu? servicios se usan en cada capa?

- a) Arquitecto de datos

- b) Analytics engineer

- c) Analista de datos

- d) Ingeniero de IA

