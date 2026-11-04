"""
CREACION DE FUNCIONES DE CONEXION

PROPOSTIO DEL SCRIPT:
    El script tiene como proposito, crear funciones de conexion para cada capa de datos, para que sea mas facil de mantener y actualizar.
"""
ruta_ADLSG2 = "abfss://almacenamiento-capas-dlh-nova-shoop@alma0dlh0nova0shop.dfs.core.windows.net/"

def obtener_ruta(capa: str)->str:
    """
    Esta función recibe como parametro la capa de datos a la que se quiere acceder y devuelve la ruta del ADLSG2 correspondiente.

    ARGS
    ----
        capa (str): Tipo de capa a la que se quiere acceder.
    RETRUN
    ------
        ruta_data (str): Ruta del ADLSG2 correspondiente a la capa.
    """
    capa = capa.lower().strip()
    match capa:
        case "bronce":
            ruta_data = ruta_ADLSG2 + ("capa-bronce")
        case "plata":
            ruta_data = ruta_ADLSG2 + ("capa-plata")
        case "oro":
            ruta_data = ruta_ADLSG2 + ("capa-oro")
        case "cuarentena":
            ruta_data = ruta_ADLSG2 + ("cuarentena")
        case _:
            raise ValueError("Valor de capa no aceptado, se espera (bronce/plata/oro/cuarentena)")
        
    return ruta_data