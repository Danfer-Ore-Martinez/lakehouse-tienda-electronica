"""
FUNCIONES DE ACTUALIZACION DE MARCAS TEMPORALES
-----------------------------------------------

PROPOSITO DEL SCRIPT
--------------------
    Funciones compartidas de actualización de marcas de agua (watermarks) para la capa plata Y oro.
"""
from pyspark.sql import functions as F, types as T, Window as W
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.session import SparkSession as SparkSessionConnect
from pyspark.sql import Row
from datetime import datetime
def obtener_ventana_fechas_carga_id(
    spark: SparkSession,
    tabla_df: DataFrame,
    nombre_tabla: str,
    nombre_schema_tabla_actualizacion: str
    ) -> tuple[datetime, datetime,Row|None, str|None,DataFrame]:
    """
     ultima_fecha_actualizacion, maxima_fecha,resumen_carga,ventana_carga_id 
    
    CALCULA LA VENTANA INCREMENTAL DE CARGA (FECHAS + IDS) DE UNA TABLA DE BRONCE
    ------------------------------------------------------------------------------------------------

    Determina qué filas de la tabla ingresa se tienen que procesar:
    Las ingeridas DESPUÉS del último watermark registrado en la tabla de control y
    HASTA la fecha máxima de bronce. También devuelve el rango de dlh_carga_id de
    esa ventana, que es el predicado REPLACE WHERE con el que se escriben cuarentena
    y plata (escrituras idempotentes: re-ejecutar la misma carga no duplica datos).

    ARGUMENTOS
    ----------
    spark : SparkSession
        Sesión de Spark activa.
    tabla_df : DataFrame
        Tabla de recursos ya leida.
    nombre_tabla : str
        Nombre de la tabla tal cual (p. ej. "customers" -> columna dlh_customers_ld
        de la tabla de control).
    nombre_schema_tabla_actualizacion : str
        Schema donde vive la tabla de control dlh_marca_ultima_carga (p. ej. "plata", "oro").

    RESULTADO
    ---------
    Tupla (ultima_fecha_actualizacion, maxima_fecha, resumen_carga, ventana_carga_id, tabla_df):
    - ultima_fecha_actualizacion: último watermark procesado en la tabla de control
      (None si la tabla nunca se ha cargado -> la primera carga procesa TODO).
    - maxima_fecha: fecha máxima de dlh_fecha_ingesta en la tabla ingresada (None si la tabla esta vacia).
    - resumen_carga: Row con id_min e id_max de dlh_carga_id dentro de la ventana
      (ambos None si la ventana quedó vacia: no hay datos nuevos que procesar).
    - ventana_carga_id: predicado "dlh_carga_id >= min AND dlh_carga_id <= max" listo
      para .option("replaceWhere", ...), o None si no hay nada que escribir.
    - tabla_df: tabla de recursos ya leida, con el filtro aplicado para la ventana.

    EJEMPLO
    -------
    ultima_fecha, max_fecha, resumen, ventana, bronce_customers = obtener_ventana_fechas_carga_id(
        spark=spark,
        tabla_df=bronce_customers,
        nombre_tabla="customers",
        nombre_schema_tabla_actualizacion="plata",
    )
    """
    # 0. Validacion
    if not isinstance(spark, (SparkSession, SparkSessionConnect)):
        raise TypeError("spark debe ser un SparkSession")
    if not isinstance(tabla_df, DataFrame):
        raise TypeError("tabla_df debe ser un DataFrame")
    
    if not isinstance(nombre_tabla, str):
        raise TypeError("nombre_tabla debe ser un str")
    elif nombre_tabla not in ("customers","products","sales_lines","web_events"):  
        raise ValueError("El nombre_tabla tiene que pertenecer a (customers/products/sales_lines/web_events)")

    if not isinstance(nombre_schema_tabla_actualizacion, str):
        raise TypeError("nombre_schema_tabla_actualizacion debe ser un str")
    elif nombre_schema_tabla_actualizacion not in ("plata","oro"):  
        raise ValueError("El nombre_schema_tabla_actualizacion tiene que pertenecer a (plata/oro)")
    # 1. Obtener fecha máxima
    maxima_fecha = (
        tabla_df
        .agg(F.max("dlh_fecha_ingesta").alias("max_fecha"))
        .first()["max_fecha"]
    )

    # 2. Obtener último Watermark procesado
    ultima_fecha_actualizacion = (
        spark.table("dlh_nova_shop."+nombre_schema_tabla_actualizacion+".dlh_marca_ultima_carga")
        .select("dlh_"+nombre_tabla+"_ld")
        .first()["dlh_"+nombre_tabla+"_ld"]
    )

    # 3. Dejar en tabla_df SOLO la ventana incremental (watermark, fecha_max_bronce]
    if maxima_fecha is None:
        tabla_df = tabla_df.limit(0)
    elif ultima_fecha_actualizacion is None:
        tabla_df = tabla_df.where(
            F.col("dlh_fecha_ingesta") <= F.lit(maxima_fecha)
        )
    else:
        tabla_df = tabla_df.where(
            (F.col("dlh_fecha_ingesta") > F.lit(ultima_fecha_actualizacion)) &
            (F.col("dlh_fecha_ingesta") <= F.lit(maxima_fecha))
        )

    # 4. Rango de ids de carga de la ventana: clave del REPLACE WHERE en cuarentena y plata
    resumen_carga = tabla_df.agg(
        F.min("dlh_carga_id").alias("id_min"),
        F.max("dlh_carga_id").alias("id_max")
    ).first()
    if resumen_carga is None or resumen_carga["id_min"] is None:
        ventana_carga_id = None
    else:
        ventana_carga_id = (
            f"dlh_carga_id  >= {resumen_carga['id_min']}"
            f" AND dlh_carga_id <= {resumen_carga['id_max']}"
        )
    return ultima_fecha_actualizacion, maxima_fecha,resumen_carga,ventana_carga_id, tabla_df


def actualizar_fechas_id_carga(
    spark: SparkSession,
    resumen_carga: Row, 
    maxima_fecha: datetime, 
    ultima_fecha_actualizacion: datetime,
    tabla: str,
    schema: str
    ) -> None:
    """
    ACTUALIZA LA MARCA DE AGUA (FECHA + ID DE CARGA) EN LA TABLA DE CONTROL
    ------------------------------------------------------------------------------------------------

    Es el "commit" de la carga: llama al stored procedure sp_actualizar_ultima_carga
    para avanzar el watermark de la tabla en dlh_marca_ultima_carga. Debe ejecutarse
    SOLO cuando las escrituras en cuarentena y plata ya terminaron sin error: si algo
    falló antes, el watermark no avanza y la próxima ejecución reprocesa la misma
    ventana de forma segura (idempotente).

    ARGUMENTOS
    ----------
    spark : SparkSession
        Sesión de Spark activa (un módulo importado no ve la variable global del notebook).
    resumen_carga : Row
        Salida de obtener_ventana_fechas_carga_id (Row con id_min e id_max); aquí se
        usa id_max como nuevo marcador de id de carga.
    maxima_fecha : timestamp
        Fecha máxima de ingesta de bronce de esta carga (nuevo watermark de fecha).
    ultima_fecha_actualizacion : timestamp o None
        Watermark anterior leído de la tabla de control (solo informativo).
    tabla : str
        Nombre de la tabla que procesa esta carga, tal como lo espera el SP
        (p. ej. "customers").
    schema : str
        Schema donde vive el SP (p. ej. "plata"), se usa para construir su nombre
        completo: dlh_nova_shop.{schema}.sp_actualizar_ultima_carga.

    RESULTADO
    ---------
    None. Imprime el avance del watermark y del id de carga, o que no hubo
    datos nuevos (en ese caso no toca la tabla de control).

    EJEMPLO
    -------
    actualizar_fechas_id_carga(
        spark=spark,
        resumen_carga=resumen_carga,
        maxima_fecha=maxima_fecha_bronce,
        ultima_fecha_actualizacion=ultima_fecha_actualizacion,
        tabla="customers",
        schema="plata",
    )
    """
    # 0. Validacion
    if not isinstance(spark, (SparkSession, SparkSessionConnect)):
        raise TypeError("spark debe ser un SparkSession")
    
    if not isinstance(resumen_carga, Row):
        raise TypeError("resumen_carga debe ser un Row")
    
    if not isinstance(maxima_fecha, datetime):
        raise TypeError("maxima_fecha debe ser un datetime")
    
    if not isinstance(ultima_fecha_actualizacion, datetime):
        raise TypeError("ultima_fecha_actualizacion debe ser un datetime")
    
    if not isinstance(tabla, str):
        raise TypeError("tabla debe ser un str")
    elif tabla not in ("customers","products","sales_lines","web_events"):  
        raise ValueError("El nombre_tabla tiene que pertenecer a (customers/products/sales_lines/web_events)")
    
    if not isinstance(schema, str):
        raise TypeError("schema debe ser un str")
    elif schema not in ("plata","oro"):  
        raise ValueError("El nombre_schema_tabla_actualizacion tiene que pertenecer a (plata/oro)")

    # Solo avanza la marca si hay datos nuevos (fecha de bronce posterior al
    # watermark actual); si no, la tabla de control queda intacta
    if (maxima_fecha is not None) and (
        ultima_fecha_actualizacion is None or maxima_fecha > ultima_fecha_actualizacion
    ):
        spark.sql(f"""
            CALL dlh_nova_shop.{schema}.sp_actualizar_ultima_carga(
                '{tabla}',
                TIMESTAMP'{maxima_fecha}',
                {resumen_carga['id_max']}
            )
        """)
        print(f"Watermark actualizado: {ultima_fecha_actualizacion} -> {maxima_fecha}")
        print(f"Id de carga máximo procesado: {resumen_carga['id_max']}")
    else:
        print(f"Sin datos nuevos en bronce: el watermark queda en {ultima_fecha_actualizacion}")