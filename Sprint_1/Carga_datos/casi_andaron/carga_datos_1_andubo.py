import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

# Leer los CSV en DataFrames
df_producto = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_Producto.csv')
df_vendor = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_Vendor.csv')
df_oc = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_OC.csv')
df_invl_inicio_year = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvInicioYear.csv')
df_inv_fin_year = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvFinYear.csv')
df_ventas = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Ventas.csv')
df_compras = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Compras.csv')

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
engine = create_engine(db_url, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

#------------------------------------------------
# Función para verificar si la tabla existe
def existe_tabla(nombre_tabla):
    query = f"SELECT OBJECT_ID('{nombre_tabla}', 'U')"
    result = session.execute(text(query)).scalar()
    return result is not None

#------------------------------------------------
# Función para crear tabla si no existe
def crear_tabla_si_no_existe(create_table_sql, nombre_tabla):
    if not existe_tabla(nombre_tabla):
        try:
            session.execute(text(create_table_sql))  # Crear la tabla
            session.commit()
            print(f"Tabla '{nombre_tabla}' creada exitosamente.")
        except SQLAlchemyError as e:
            print(f"Error al crear la tabla '{nombre_tabla}': {e}")
            session.rollback()
    else:
        print(f"La tabla '{nombre_tabla}' ya existe. No se creará nuevamente.")

#------------------------------------------------
# Función para eliminar tabla si existe
def eliminar_tabla_si_existe(nombre_tabla):
    if existe_tabla(nombre_tabla):
        try:
            session.execute(text(f"DROP TABLE {nombre_tabla}"))
            session.commit()
            print(f"Tabla '{nombre_tabla}' eliminada exitosamente.")
        except SQLAlchemyError as e:
            print(f"Error al eliminar la tabla '{nombre_tabla}': {e}")
            session.rollback()

#------------------------------------------------
# Función para verificar claves foráneas
def verificar_claves_foraneas(df, ordenes_existentes, productos_existentes):
    if 'ProductoID' in df.columns:
        df['ProductoID'] = df['ProductoID'].where(df['ProductoID'].isin(productos_existentes), np.nan)
        
    # Solo verifica OrdenCompraID si está presente en el DataFrame
    if 'OrdenCompraID' in df.columns:
        df['OrdenCompraID'] = df['OrdenCompraID'].where(df['OrdenCompraID'].isin(ordenes_existentes), np.nan)

    # Eliminar filas con nulos en las claves foráneas
    claves_a_verificar = ['ProductoID']
    
    if 'OrdenCompraID' in df.columns:
        claves_a_verificar.append('OrdenCompraID')
        
    return df.dropna(subset=claves_a_verificar)

#------------------------------------------------
# Función para cargar datos en la tabla con manejo de errores de inserción
def cargar_datos(df, nombre_tabla):
    if not df.empty:  # Solo intenta cargar si hay datos válidos
        try:
            df.to_sql(nombre_tabla, con=engine, if_exists='append', index=False)
            print(f"Datos cargados exitosamente en '{nombre_tabla}'.")
        except IntegrityError as e:
            print(f"Error de integridad al insertar datos en '{nombre_tabla}': {e}")
            session.rollback()  # Deshacer cambios si hay un error
        except SQLAlchemyError as e:
            print(f"Error al insertar datos en '{nombre_tabla}': {e}")
            session.rollback()
    else:
        print(f"No hay datos válidos para insertar en '{nombre_tabla}'.")

#------------------------------------------------
# Función para eliminar duplicados en claves primarias
def eliminar_duplicados(df, clave_primaria):
    if clave_primaria in df.columns:
        df.drop_duplicates(subset=[clave_primaria], keep='first', inplace=True)
    return df

#------------------------------------------------
# Función para procesar DataFrames
def procesar_dataframes(dfs, create_table_sqls, nombres_tablas):
    # Obtener productos y órdenes existentes antes de procesar
    productos_existentes = session.execute(text("SELECT ProductoID FROM dbo.Dim_Producto")).fetchall()
    productos_existentes = {p[0] for p in productos_existentes}

    ordenes_existentes = session.execute(text("SELECT OrdenCompraID FROM dbo.Dim_OC")).fetchall()
    ordenes_existentes = {o[0] for o in ordenes_existentes}

    for df, create_table_sql, nombre_tabla in zip(dfs, create_table_sqls, nombres_tablas):
        # Eliminar tabla si existe
        eliminar_tabla_si_existe(nombre_tabla)

        # Crear tabla si no existe
        crear_tabla_si_no_existe(create_table_sql, nombre_tabla)

        # Convertir columnas a tipos adecuados
        for date_col in ['ReceivingDate', 'PayDate', 'PODate', 'InvoiceDate', 'SalesDate', 'StartDate', 'EndDate']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        # Eliminar duplicados en claves primarias
        if nombre_tabla == 'Dim_Producto':
            df = eliminar_duplicados(df, 'ProductoID')
        elif nombre_tabla == 'DimVendor':
            df = eliminar_duplicados(df, 'VendorID')
        elif nombre_tabla == 'Dim_OC':
            df = eliminar_duplicados(df, 'OrdenCompraID')
        elif nombre_tabla == 'Fact_Ventas':
            df = eliminar_duplicados(df, 'InventoryId')
        elif nombre_tabla == 'Fact_Compras':
            df = eliminar_duplicados(df, 'InventoryId')

        # Verificar claves foráneas y eliminar nulos
        if nombre_tabla in ['Fact_Compras', 'Fact_Ventas']:  # Solo verifica claves en estas tablas
            df = verificar_claves_foraneas(df, ordenes_existentes, productos_existentes)

        # Eliminar filas con nulos en cualquier columna relevante (opcional)
        df.dropna(inplace=True)

        # Cargar datos
        cargar_datos(df, nombre_tabla)

#------------------------------------------------
# Crear tablas (asegúrate de que las definiciones estén correctas)
create_table_sqls = [
    '''
    CREATE TABLE dbo.DimVendor (
        VendorID INT PRIMARY KEY,
        VendorNumber INT,
        VendorName VARCHAR(255)
    );
    ''',
    '''
    CREATE TABLE dbo.Dim_Producto (
        ProductoID INT PRIMARY KEY,
        Brand INT,
        Description VARCHAR(255),
        Price FLOAT,
        Size VARCHAR(255),
        Volume FLOAT,
        Classification INT,
        PurchasePrice FLOAT,
        VendorID INT,
        FOREIGN KEY (VendorID) REFERENCES dbo.DimVendor(VendorID)
    );
    ''',
    '''
    CREATE TABLE dbo.Dim_OC (CREATE TABLE dbo.Fact_Ventas (
        OrdenCompraID INT PRIMARY KEY,
        PONumber INT,
        PODate DATE,
        InvoiceDate DATE,
        PayDate DATE,
        Quantity FLOAT,
        Dollars FLOAT,
        Freight FLOAT,
        VendorNumber INT
    );
    ''',
    '''
    CREATE TABLE dbo.Fact_Ventas (
        InventoryId VARCHAR (255) PRIMARY KEY,
        Store VARCHAR(255),
        SalesQuantity INT,
        SalesDollars FLOAT,
        SalesPrice FLOAT,
        SalesDate DATE,
        ExciseTax FLOAT,
        ProductoID INT,
        FOREIGN KEY (ProductoID) REFERENCES dbo.Dim_Producto(ProductoID)
    );
    ''',
    '''
    CREATE TABLE dbo.Fact_Compras (
        InventoryId VARCHAR(255) INT PRIMARY KEY,
        Store VARCHAR(255),
        ReceivingDate DATE,
        Quantity FLOAT,
        Dollars FLOAT,
        OrdenCompraID INT,
        ProductoID INT,
        FOREIGN KEY (ProductoID) REFERENCES dbo.Dim_Producto(ProductoID),
        FOREIGN KEY (OrdenCompraID) REFERENCES dbo.Dim_OC(OrdenCompraID)
    );
    '''
]

nombres_tablas = ['DimVendor', 'Dim_Producto', 'Dim_OC', 'Fact_Ventas', 'Fact_Compras']

# Procesar todos los DataFrames
procesar_dataframes([df_vendor, df_producto, df_oc, df_ventas, df_compras], create_table_sqls, nombres_tablas)

# Cerrar la sesión
session.close()
