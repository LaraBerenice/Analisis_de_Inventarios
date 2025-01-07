import pandas as pd
import os
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las variables de entorno
hostname = os.getenv("DB_HOST")
db = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# Cambiar la cadena de conexión para SQL Server
db_url = f"mssql+pyodbc://{user}:{password}@{hostname}/{db}?driver=ODBC+Driver+17+for+SQL+Server"

# Crear el motor de base de datos
engine = create_engine(db_url)

def verificar_claves_primas_y_foraneas():
    # Crear un inspector para obtener metadatos de la base de datos
    inspector = inspect(engine)
    
    # Obtener todas las tablas en la base de datos
    tablas = inspector.get_table_names()
    
    for tabla in tablas:
        print(f"\nVerificando tabla: {tabla}")

        # Obtener las claves primarias
        pk_constraint = inspector.get_pk_constraint(tabla)
        claves_primas = pk_constraint['constrained_columns'] if pk_constraint else []
        print(f"Claves primarias en '{tabla}': {claves_primas}")

        # Verificar claves autoincrementales
        columnas = inspector.get_columns(tabla)
        claves_autoincrementales = [col['name'] for col in columnas if col.get('autoincrement', False)]
        
        if claves_autoincrementales:
            print(f"Claves autoincrementales en '{tabla}': {claves_autoincrementales}")
        else:
            print(f"No se encontraron claves autoincrementales en '{tabla}'.")

        # Cargar datos de la tabla
        df = pd.read_sql_table(tabla, con=engine)

        # Verificar duplicados en claves primarias
        for clave in claves_primas:
            duplicados = df[clave][df[clave].duplicated()].unique()
            if len(duplicados) > 0:
                print(f"Duplicados encontrados en clave primaria '{clave}': {duplicados}")
            else:
                print(f"No se encontraron duplicados en clave primaria '{clave}'.")

        # Obtener claves foráneas
        claves_foraneas = inspector.get_foreign_keys(tabla)
        for fk in claves_foraneas:
            nombre_columna = fk['constrained_columns'][0]
            tabla_referenciada = fk['referred_table']
            columna_referenciada = fk['referred_columns'][0]

            # Comprobar que la tabla referenciada existe
            if inspector.has_table(tabla_referenciada):
                print(f"Comprobando clave foránea '{nombre_columna}' que referencia a '{tabla_referenciada}.{columna_referenciada}'.")

                # Cargar datos de la tabla referenciada
                df_referenciada = pd.read_sql_table(tabla_referenciada, con=engine)

                # Verificar datos huérfanos
                huérfanos = df[~df[nombre_columna].isin(df_referenciada[columna_referenciada])]
                if len(huérfanos) > 0:
                    print(f"Datos huérfanos encontrados en '{tabla}', columna '{nombre_columna}':")
                    print(huérfanos)
                else:
                    print(f"No se encontraron datos huérfanos en '{tabla}', columna '{nombre_columna}'.")

            else:
                print(f"Tabla referenciada '{tabla_referenciada}' no existe.")

# Ejecutar la verificación
verificar_claves_primas_y_foraneas()

