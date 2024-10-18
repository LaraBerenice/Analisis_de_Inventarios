import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Leer los CSV en DataFrames
df_producto = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_Producto.csv')
df_vendor = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_Vendor.csv')
df_oc = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_OC.csv')
df_ventas = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Ventas_ult.csv')
df_compras = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Compras_ult.csv')
df_inv_inicio = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvInicioYear_ult.csv')
df_inv_fin_year = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvFinYear_ult.csv')

# Cargar las variables de entorno
load_dotenv()

# Obtener las variables de entorno
hostname = os.getenv("DB_HOST")
db = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# Cambiar la cadena de conexión para SQL Server
db_url = f"mssql+pyodbc://{user}:{password}@{hostname}/{db}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_url, echo=False)
Session = sessionmaker(bind=engine)

# Función para crear tablas en la base de datos
def crear_tablas(sql_statements):
    with Session() as session:
        for sql in sql_statements:
            try:
                session.execute(text(sql))
                session.commit()
                print("Tabla creada con éxito.")
            except SQLAlchemyError as e:
                print(f"Error al crear la tabla: {str(e)}")
                session.rollback()

# Función para validar y limpiar datos
def validar_datos(df, tabla):
    # Verificar valores nulos y eliminar filas con nulos
    if df.isnull().values.any():
        print(f"Advertencia: Se encontraron valores nulos en {tabla}. Se eliminarán.")
        df = df.dropna()

    # Eliminar duplicados en las claves primarias según la tabla correspondiente
    if tabla == 'Dim_Producto':
        df = df.drop_duplicates(subset=['ProductoID'], keep='first')
    elif tabla == 'DimVendor':
        df = df.drop_duplicates(subset=['VendorID'], keep='first')
    elif tabla == 'Dim_OC':
        df = df.drop_duplicates(subset=['OrdenCompraID'], keep='first')
    elif tabla == 'Fact_Compras':
        if 'ComprasID' in df.columns:
            df = df.drop_duplicates(subset=['ComprasID'], keep='first')  # Clave primaria correcta
    elif tabla == 'Fact_InvInicio':
        if 'InvInicioID' in df.columns:
            df = df.drop_duplicates(subset=['InvInicioID'], keep='first')  # Clave primaria correcta
    elif tabla == 'Fact_InvFinYear':
        if 'InvFinYearID' in df.columns:
            df = df.drop_duplicates(subset=['InvFinYearID'], keep='first')  # Clave primaria correcta

    # Cargar IDs necesarios desde la base de datos
    try:
        producto_ids = pd.read_sql("SELECT ProductoID FROM dbo.Dim_Producto", engine)
    except SQLAlchemyError as e:
        print(f"Error al cargar IDs necesarios: {str(e)}")
        return pd.DataFrame()

    # Verificar claves foráneas
    print(f"Número de filas antes de filtrar {tabla}: {df.shape[0]}")
    if tabla == 'Fact_Ventas':
        pass  # No se hace nada para 'Fact_Ventas' aquí
    elif tabla == 'Fact_Compras':
        orden_compra_ids = pd.read_sql("SELECT OrdenCompraID FROM dbo.Dim_OC", engine)
        df = df[df['OrdenCompraID'].isin(orden_compra_ids['OrdenCompraID'])]
        df = df[df['ProductoID'].isin(producto_ids['ProductoID'])]
    elif tabla == 'Fact_InvInicio':
        df = df[df['ProductoID'].isin(producto_ids['ProductoID'])]
    elif tabla == 'Fact_InvFinYear':
        df = df[df['ProductoID'].isin(producto_ids['ProductoID'])]
    print(f"Número de filas después de filtrar {tabla}: {df.shape[0]}")

    return df

# Función para insertar datos en la base de datos
def insertar_datos(df, tabla):
    df_validado = validar_datos(df, tabla)

    # Definir columnas autoincrementales según la tabla
    columnas_autoincrementales = {
        'Fact_Ventas': ['VentasID'],
        'Fact_Compras': ['ComprasID'],
        'Fact_InvInicio': ['InvInicioID'],
        'Fact_InvFinYear': ['InvFinYearID']
    }

    # Elimina columnas autoincrementales solo si existen
    if tabla in columnas_autoincrementales:
        for col in columnas_autoincrementales[tabla]:
            if col in df_validado.columns:
                df_validado = df_validado.drop(columns=[col])

    if not df_validado.empty:
        try:
            with Session() as session:
                df_validado.to_sql(tabla, engine, if_exists='append', index=False)
                session.commit()
                print(f"Datos insertados correctamente en la tabla {tabla}.")
        except SQLAlchemyError as e:
            print(f"Error al insertar datos en {tabla}: {str(e)}")
            session.rollback()
    else:
        print(f"Advertencia: No se insertaron datos en {tabla} debido a la validación.")

# Insertar los datos en las tablas
insertar_datos(df_producto, 'Dim_Producto')
insertar_datos(df_vendor, 'DimVendor')
insertar_datos(df_oc, 'Dim_OC')
insertar_datos(df_ventas, 'Fact_Ventas')
insertar_datos(df_compras, 'Fact_Compras')
insertar_datos(df_inv_inicio, 'Fact_InvInicio')
insertar_datos(df_inv_fin_year, 'Fact_InvFinYear')
