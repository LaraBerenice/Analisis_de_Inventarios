-- Seleccionar la base de datos brindis_real
USE brindis_real;
GO

-- Cargar datos en la tabla FactVentas
BULK INSERT FactVentas
FROM '"C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Ventas.csv"'  -- Especifica la ruta completa de tu archivo CSV
WITH (
    FIELDTERMINATOR = ',',  -- Separador de campos
    ROWTERMINATOR = '\n',   -- Separador de filas
    FIRSTROW = 2,           -- Salta el encabezado
    TABLOCK
);
GO

-- Cargar datos en la tabla DimProducto
BULK INSERT DimProducto
FROM '"C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_Producto.csv"'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    TABLOCK
);
GO

-- Cargar datos en la tabla DimVendor
BULK INSERT DimVendor
FROM '"C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_Vendor.csv"'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    TABLOCK
);
GO

-- Cargar datos en la tabla DimOC
BULK INSERT DimOC
FROM '"C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Dim_OC.csv"'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    TABLOCK
);
GO

-- Cargar datos en la tabla FactCompras
BULK INSERT FactCompras
FROM '"C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_Compras.csv"'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    TABLOCK
);
GO

-- Cargar datos en la tabla Fact_inicio_inv_inventario
BULK INSERT Fact_inicio_inv_inventario
FROM '"C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvInicioYear.csv"'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    TABLOCK
);
GO

-- Cargar datos en la tabla Fact_Fin_inv_inventario
BULK INSERT Fact_Fin_inv_inventario
FROM '"C:\Users\beren\Desktop\HENRRY\G4_Inventory\archive\CSV_ULTIMOS\Fact_InvFinYear.csv"'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    TABLOCK
);
GO
