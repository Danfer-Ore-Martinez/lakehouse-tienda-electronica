# **Convenciones de Nomenclatura**

Este documento describe las convenciones de nomenclatura utilizadas para schemas, tables, views, columns y otros objetos en el Data LakeHouse.

## **Tabla de Contenido**

1. [Principios Generales](#Principios-generales)
2. [Convenciones de Nomenclatura para Tablas](#Convenciones-de-Nomenclatura-para-Tablas)
   - [Reglas Capa Bronce](#Reglas-Capa-Bronce)
   - [Reglas Capa Plata](#Reglas-Capa-Plata)
   - [Reglas Capa Oro](#Reglas-Capa-Oro)
3. [Convenciones de Nomenclatura para Columnas](#Convenciones-de-Nomenclatura-para-Columnas)
   - [Llaves Sustitutas](#Llaves-Sustitutas)
   - [Columnas Técnicas](#Columnas-Técnicas)
4. [Stored Procedure](#Stored-Procedure)
---

## **Principios Generales** 

- **Convenciones de Nomenclatura**: Usar snake_case y todo en minusculas.
- **Idioma**: Usar Español para los nombres de tablas, columnas, carpetas y archivos, sin embargo, las capas bronce y plata tendrán los nombres de los recursos originales.
- **Sobre las Palabras Reservadas**: No utilizar palabras reservadas de PySpark o SQLSpark.

## **Convenciones de Nomenclatura para Tablas**

### **Reglas Capa Bronce**
- Todas las tablas mantienen sus nombre originales, pero, en minúscula.
- **`<entidad>`**  
  - `<entidad>`: El nombre exacto del archivo.
  - Ejemplo: `customers` → Información del cliente.

### **Reglas Capa Plata**
- Todas las tablas mantienen sus nombre originales, pero, en minúscula.
- **`entidad>`**  
  - `<entidad>`: El nombre exacto del archivo.
  - Ejemplo: `products` → Información sobre productos.

### **Reglas Capa Oro**
- Todos los nombres tienen que estar relacionados con el negocio.
- **`<categoria>_<entidad>`**  
  - `<categoria>`: Describe el rol de la tabla, por ejemplo, `dim`  o `fact`.  
  - `<entidad>`: Describe el nombre de la tabla, siguiendo la logica y estructura del negocio (ejemplo:, `clientes`, `productos`, `pagos`).  
  - ejemplos:
    - `dim_clientes` → Tabla dimension de la información del cliente.  
    - `fact_pagos` → Tabla fact que almacena la información de los pagos  

#### **Diccionario de pre-fijos**

| Pre-fijo    | Significado                      | Ejemplo(s)                                |
|-------------|----------------------------------|-------------------------------------------|
| `dim_`      | Tabla de Dimension               | `dim_clientes`, `dim_productos`           |
| `fact_`     | Tabla de Fact                    | `fact_pagos`                              |

## **Convenciones de Nomenclatura para Columnas**

### **Llaves Sustitutas**  
- Todas las llaves primarias en las tablas dimension tienen el sufijo `_llave`.
- **`<nombre_tabla>_llave`**  
  - `<nombre_tabla>`: Se refiere a la tabla a la que pertenece la llave.
  - `_llave`: Un sufijo indicando que esta es una llave sustituta.  
  - Ejemplo: `cliente_llave` → Llave sustitua en la tabla `dim_cliente`.
  
### **Columnas Técnicas**
- Todas las columnas tecnicas comienzan con el prefijo `dlh_`, seguido del nombre de la columna que explica el contenido de esta.
- **`dlh__<nombre_columna>`**  
  - `dlh_`: Prefijo creado especifícamente para la meta-data generada por el sistema.  
  - `<nombre_columna>`: Describe el nombre del contenido que esta columna representa.  
  - Ejemplo: `dlh_fecha_carga` → Columna que genera el sistema para indicar en que fecha se cargo ese registro.