"""
FUNCIONES DE LIMPIEZA DE DATOS
------------------------------

PROPOSITO DEL SCRIPT
--------------------
    Funciones compartidas que se utilizan para la limpieza de datos. 
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column

def perfilar_data_frame(df: DataFrame) -> DataFrame:
    """"
    Esta funcion se encarga de crear un perfilado del Data Frame
    ------------------------------------------------------------
    
    Argumentos 
    ----------
        df (DataFrame): Data frame sobre el cuál se quiere realizar un perfilado de datos.
    
    Resultado
    ---------
        Una df con las siguientes columnas 
        - nombre_columna: Nombre de la columna ingresada.   
        - tipo_dato: Tipo de dato de la columna.
        - n_nulls: Número de nulos de la columna.
        - pct_nullls   : Porcentaje de números nulos en toda la columna.  
        - n_distinct: Número de registros distintos
        - min: Valor mínimo de la columna
        - max: Valor máximo de la columna 
    Ejemplo de uso 
    --------------
        df = spark.createDataFrame([
            ("jose",2),
            ("maria",None)
        ], ["nombre","edad"])
        perfilado(df).show()
        ------------------------------------------------------------------------
        |nombre_columna|tipo_dato|n_nulls| pct_nullls  |n_distinct| min |  max |
        |--------------|---------|-------|-------------|----------|-----|------|
        |  'nombre'    | 'string'|    0  |     0       |    2     |null | null |
        |   'edad'     |  'int'  |    1  |     50      |    2     | 22  |  22  |
        |--------------|---------|-------|-------------|----------|-----|------|
    """
    if not isinstance(df, DataFrame):
        raise TypeError("El argumento df debe ser un DataFrame de Spark")
    
    resultados = []
    
    for nombre, tipo in df.dtypes:

        columna = F.col(nombre)

        # Limpieza para columnas string
        if tipo == "string":
            df_limpio = df.withColumn(
                nombre,
                F.when(
                    (F.trim(columna) == "")
                    | (columna.isin("N/A","n/a", "NULL", "null", "-")),
                    None
                ).otherwise(columna)
            )
        else:
            df_limpio = df

        # Métricas comunes
        agregaciones = [
            F.count(F.when(F.col(nombre).isNull(), 1)).alias("n_nulls"),
            F.round(F.count(F.when(F.col(nombre).isNull(), 1)) / F.count("*") * 100,2).alias("pct_nulls"),
            F.approx_count_distinct(F.col(nombre)).alias("n_distinct")
        ]

        # Solo min/max para columnas que no sean string ni boolean
        if tipo not in ("string", "boolean"):
            agregaciones.extend([
                F.min(F.col(nombre)).cast("string").alias("min"),
                F.max(F.col(nombre)).cast("string").alias("max")
            ])
        else:
            agregaciones.extend([
                F.lit(None).cast("string").alias("min"),
                F.lit(None).cast("string").alias("max")
            ])

        resultado = (
            df_limpio
            .agg(*agregaciones)
            .withColumn("nombre_columna", F.lit(nombre))
            .withColumn("tipo_dato", F.lit(tipo))
            .select(
                "nombre_columna",
                "tipo_dato",
                "n_nulls",
                "pct_nulls",
                "n_distinct",
                "min",
                "max"
            )
        )

        resultados.append(resultado)

    # Unir todas las filas
    perfil_final = resultados[0]

    for resultado in resultados[1:]:
        perfil_final = perfil_final.unionByName(resultado)

    return perfil_final

def limpiar_texto(columna: Column)->Column:
    """
    ESTA FUNCIÓN SE ENCARGA DE LIMPIAR EL FORMATO DE LOS TEXTOS
    -----------------------------------------------------------
        
    Argumentos
    ----------
        columna (Column): Columna de df a transformar. 
        
    Resultado
    --------- 
        columna_final (Column): Columna transformada.

    Ejemplo
    -------
        df = spark.createDataFrame([
            ("APP"),
            ("crm_hhrr"),
            ("WEB")
        ], ["recurso"])
        limpiar_texto(df.recurso)
        >>> ["App", "CRM Hhrr", "Web")
    """
    if not isinstance(columna, Column):
        raise TypeError("El argumento columna debe ser un Column de Spark")
    conectores = {
        "de", "del", "la", "las", "el", "los",
        "y", "e", "o", "u", "a", "en", "con",
        "por","to", "para","on"
    }

    # Toda la palabra en una lista
    palabras = F.split(
        F.lower(F.trim(columna)),
        r"\s+"
    )

    # Recorre la lista y aplica initcap, si es el primer valor o si no esta en la lista de conectores
    palabras_limpias = F.transform(
        palabras,
        lambda palabra, i:
            F.when(
                palabra.isin(["usb","usb-c"]),
                F.upper(palabra)
            ).when(
                (i == 0) | (~palabra.isin(*conectores)),
                F.initcap(palabra)
            ).otherwise(palabra)
    )
    
    columna_final = F.when(
                F.isnull(columna) | (F.lower(F.trim(columna)).isin("n/a","", "null", "-")),
                F.lit("N/A")
            ).otherwise(
                F.concat_ws(" ", palabras_limpias)
            )
    return columna_final

def estandarizar_columnas_str(df: DataFrame) -> DataFrame: 
    """
    ESTA FUNCIÓN SE ENCARGA DE ESTANDARIZAR TODAS LAS  COLUMNAS STRING DE UN DATAFRAME
    ----------------------------------------------------------------------------------
        
    Argumentos
    ----------
        df (DataFrame): DataFrame a evaluar.
        
    Resultado
    --------- 
        df_final (DataFrame): DataFrame Transformado.

    Ejemplo
    -------
        df = spark.createDataFrame([
            ("APP","n/a"),
            ("crm_hhrr",None),
            ("    ","-")
        ], ["recurso","caracteristica"])
        estandarizar_columnas_str(df).show()
        -------------------------------
        |   recurso  | caracteristica |
        |------------|----------------|
        | 'APP'      |  'N/A'         |
        | 'crm_hhrr' |  'N/A'         | 
        | 'N/A'      |  'N/A'         |
        |------------|----------------|
    """
    if not isinstance(df, DataFrame):
        raise TypeError("El argumento df debe ser un DataFrame de Spark")
    df_limpio = df 
    for nombre, tipo in df_limpio.dtypes:

        columna = F.col(nombre)

        # Limpieza para columnas string
        if tipo == "string":
            df_limpio = df_limpio.withColumn(
                nombre,
                F.when(
                    (F.lower(F.trim(columna)).isin("","n/a", "null", "-"))
                    | (columna.isNull()),
                    'N/A'
                ).otherwise(columna)
            )
    return df_limpio
from pyspark.sql import functions as F

def generar_hash(df: DataFrame, columnas_hash: list[str], nombre_hash: str) -> DataFrame:
    """
    ESTA FUNCION SE ENCARGA DE GENERAR UN HASH A PARTIR DE UNA LISTA DE COLUMNAS
    ----------------------------------------------------------------------------
        
    Argumentos
    ----------
        df (DataFrame): DataFrame a evaluar.
        columnas_hash (List[str]): Lista de columnas a tomar para el hash.
        nombre_hash (str): Nombre de la columna hash.
        
    Resultado
    --------- 
        df_final (DataFrame): DataFrame con la nueva columna hash.

    Ejemplo
    -------
        df = spark.createDataFrame([
            ("APP","n/a"),
            ("crm_hhrr",None),
            ("    ","-")
        ], ["recurso","caracteristica"])
        generar_hash(df, ["recurso","caracteristica"], "hash").show()
        -------------------------------------------------------------------------------------------------
        |   recurso  | caracteristica |                              hash                               |
        |------------|----------------|-----------------------------------------------------------------|
        | "APP"      |      "n/a"     | 61c8314a3988d412bd5ce9dfd84ae67b87aea492d700e1fef4c7553e135b5b0d|
        | "crm_hhrr" |      None      | c662ef632606459682fd2e9f0f748952279c8c9c90f2a047bc4763365fdf98e1|
        | "    "     |      "-"       | 129456f2c05794b16571f07e5198583e66362dfbec62710c782b56098175bf0c|
        |------------|----------------|-----------------------------------------------------------------|
    """
    if not isinstance(df, DataFrame):
        raise TypeError("El argumento df debe ser un DataFrame de Spark")

    if not all(isinstance(c, str) for c in columnas_hash):
        raise TypeError("El argumento columnas_hash debe ser una lista de columnas de tipo string")
    
    if not isinstance(nombre_hash, str):
        raise TypeError("El argumento nombre_hash debe ser un string")
    
    contenido = F.to_json(
        F.struct(
            *[
                F.col(c).alias(c)
                for c in columnas_hash
            ]
        ),
        options={"ignoreNullFields": "false"}
    )

    return df.withColumn(
        nombre_hash,
        F.sha2(contenido, 256)
    )

def fecha_de_corte_data() :
    """Retorna la fecha de corte dinámica usando la fecha actual del sistema."""
    return F.current_timestamp()