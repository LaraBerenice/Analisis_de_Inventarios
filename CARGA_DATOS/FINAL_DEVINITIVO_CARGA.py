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
df_ventas = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Ventas.csv')
df_compras = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Compras.csv')
df_inv_inicio = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvInicioYear.csv')
df_inv_fin_year = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvFinYear.csv')

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

# Crear tablas con claves autoincrementales correctamente definidas
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
    CREATE TABLE dbo.Dim_OC (
        OrdenCompraID INT PRIMARY KEY,
        PONumber INT,
        PODate DATE,
        InvoiceDate DATE,
        PayDate DATE,
        Quantity FLOAT,
        Dollars FLOAT,
        Freight FLOAT,
        VendorNumber INT,
        VendorName VARCHAR(255)
    );
    ''',
    '''
    CREATE TABLE dbo.Fact_Ventas (
        VentasID INT PRIMARY KEY IDENTITY(1,1),  -- Definido como autoincremental
        InventoryId VARCHAR(255),
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
        ComprasID INT PRIMARY KEY IDENTITY(1,1),  -- Definido como autoincremental
        InventoryId VARCHAR(255),
        Store VARCHAR(255),
        ReceivingDate DATE,
        Quantity FLOAT,
        Dollars FLOAT,
        OrdenCompraID INT,
        ProductoID INT,
        FOREIGN KEY (ProductoID) REFERENCES dbo.Dim_Producto(ProductoID),
        FOREIGN KEY (OrdenCompraID) REFERENCES dbo.Dim_OC(OrdenCompraID)
    );
    ''',
    '''
    CREATE TABLE dbo.Fact_InvInicio (
        InvInicioID INT PRIMARY KEY IDENTITY(1,1),
        InventoryId VARCHAR(255),
        Store VARCHAR(255),
        City VARCHAR(255),
        onHand FLOAT,
        startDate DATE,
        ProductoID INT,
        FOREIGN KEY (ProductoID) REFERENCES dbo.Dim_Producto(ProductoID)
    );
    ''',
    '''
    CREATE TABLE dbo.Fact_InvFinYear (
        InvFinYearID INT PRIMARY KEY IDENTITY(1,1),
        InventoryId VARCHAR(255),
        Store VARCHAR(255),
        City VARCHAR(255),
        onHand FLOAT,
        endDate DATE,
        ProductoID INT,
        FOREIGN KEY (ProductoID) REFERENCES dbo.Dim_Producto(ProductoID)
    );
    '''
]

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

# Crear las tablas
crear_tablas(create_table_sqls)

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
    elif tabla == 'Fact_Ventas':
        df = df.drop_duplicates(subset=['InventoryId'], keep='first')
    elif tabla == 'Fact_Compras':
        df = df.drop_duplicates(subset=['InventoryId'], keep='first')
    elif tabla == 'Fact_InvInicio':
        df = df.drop_duplicates(subset=['InventoryId', 'Store', 'City', 'onHand', 'startDate', 'ProductoID'], keep='first')
    elif tabla == 'Fact_InvFinYear':
        df = df.drop_duplicates(subset=['InventoryId', 'Store', 'City', 'onHand', 'endDate', 'ProductoID'], keep='first')

    # Cargar IDs necesarios desde la base de datos
    try:
        producto_ids = pd.read_sql("SELECT ProductoID FROM dbo.Dim_Producto", engine)
    except SQLAlchemyError as e:
        print(f"Error al cargar IDs necesarios: {str(e)}")
        return pd.DataFrame()

    # Verificar claves foráneas
    print(f"Número de filas antes de filtrar {tabla}: {df.shape[0]}")
    if tabla == 'Fact_Ventas':
        df = df[df['ProductoID'].isin(producto_ids['ProductoID'])]
    elif tabla == 'Fact_Compras':
        orden_compra_ids = pd.read_sql("SELECT OrdenCompraID FROM dbo.Dim_OC", engine)
        df = df[df['OrdenCompraID'].isin(orden_compra_ids['OrdenCompraID'])]
        df = df[df['ProductoID'].isin(producto_ids['ProductoID'])]
    elif tabla == 'Fact_InvInicio':
        df = df[df['ProductoID'].isin(producto_ids['ProductoID'])]  # Verificar ProductoID para Fact_InvInicio
    elif tabla == 'Fact_InvFinYear':
        df = df[df['ProductoID'].isin(producto_ids['ProductoID'])]  # Verificar ProductoID para Fact_InvFinYear
    print(f"Número de filas después de filtrar {tabla}: {df.shape[0]}")

    return df

# Función para insertar datos en la base de datos
def insertar_datos(df, tabla):
    df_validado = validar_datos(df, tabla)

    # Elimina columnas autoincrementales solo si existen
    if tabla == 'Fact_Ventas':
        df_validado = df_validado.drop(columns=['VentasID'], errors='ignore')  # Evitar la columna autoincremental
    elif tabla == 'Fact_Compras':
        df_validado = df_validado.drop(columns=['ComprasID'], errors='ignore')  # Evitar la columna autoincremental
    elif tabla == 'Fact_InvInicio':
        df_validado = df_validado.drop(columns=['InvInicioID'], errors='ignore')
    elif tabla == 'Fact_InvFinYear':
        df_validado = df_validado.drop(columns=['InvFinYearID'], errors='ignore')

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
