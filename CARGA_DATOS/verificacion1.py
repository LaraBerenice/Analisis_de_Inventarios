import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Cargar los archivos CSV de las demás tablas
df_producto = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_Producto.csv')
df_oc = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_OC.csv')
df_compras = pd.read_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Compras.csv')

# Función para generar fechas aleatorias dentro de un rango
def generar_fecha_aleatoria(fecha_inicio, fecha_fin):
    delta = fecha_fin - fecha_inicio
    int_delta = delta.days
    random_day = np.random.randint(0, int_delta)
    return fecha_inicio + timedelta(days=random_day)

# Generar el DataFrame para la tabla de ventas
def generar_ventas(num_filas):
    # Crear un DataFrame vacío
    df_ventas = pd.DataFrame()
    
    # Asignar ProductoID desde Dim_Producto
    df_ventas['ProductoID'] = np.random.choice(df_producto['ProductoID'], num_filas)
    
    # Asignar OrdenCompraID desde Dim_OC
    df_ventas['OrdenCompraID'] = np.random.choice(df_oc['OrdenCompraID'], num_filas)
    
    # Asignar InventoryId desde Fact_Compras
    df_ventas['InventoryId'] = np.random.choice(df_compras['InventoryId'], num_filas)
    
    # Generar Store aleatorio (por ejemplo, entre 1 y 10)
    df_ventas['Store'] = np.random.randint(1, 11, size=num_filas)
    
    # Generar SalesQuantity aleatorio (por ejemplo, entre 1 y 100)
    df_ventas['SalesQuantity'] = np.random.randint(1, 101, size=num_filas)
    
    # Generar SalesDollars aleatorio (por ejemplo, entre 10 y 5000)
    df_ventas['SalesDollars'] = np.random.uniform(10, 5000, size=num_filas).round(2)
    
    # Generar SalesPrice aleatorio (por ejemplo, entre 1 y 100)
    df_ventas['SalesPrice'] = np.random.uniform(1, 100, size=num_filas).round(2)
    
    # Generar fechas de venta entre el 1 de enero de 2020 y el 31 de diciembre de 2023
    fecha_inicio = datetime(2020, 1, 1)
    fecha_fin = datetime(2023, 12, 31)
    df_ventas['SalesDate'] = [generar_fecha_aleatoria(fecha_inicio, fecha_fin) for _ in range(num_filas)]
    
    # Generar ExciseTax aleatorio (por ejemplo, entre 0.05 y 0.20)
    df_ventas['ExciseTax'] = np.random.uniform(0.05, 0.20, size=num_filas).round(2)
    
    # Devolver el DataFrame generado
    return df_ventas

# Generar datos de ventas (por ejemplo, 1000 filas)
df_ventas = generar_ventas(1000)

# Guardar el DataFrame generado en un archivo CSV (sin la columna VentasID)
df_ventas.to_csv(r'C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Ventas.csv', index=False)

print("El archivo Fact_Ventas.csv ha sido generado exitosamente sin la columna VentasID.")
