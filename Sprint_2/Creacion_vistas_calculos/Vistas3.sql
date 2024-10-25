CREATE VIEW V_Ciudades_Mas_Ventas AS
SELECT 
    i.City AS Ciudad,
    FORMAT(SUM(v.SalesDollars), 'N2') AS TotalVentas
FROM 
    Fact_Ventas v
JOIN 
    Fact_InvInicio i ON v.ProductoID = i.ProductoID
GROUP BY 
    i.City;

--------------------------------------------------------------------
CREATE VIEW V_Top5_Productos_Mayor_Inventario AS
SELECT TOP 5
    p.Description AS Producto,
    FORMAT(SUM(i.onHand * p.PurchasePrice), 'N2') AS ValorInventario
FROM 
    Fact_InvInicio i
JOIN 
    Dim_Producto p ON i.ProductoID = p.ProductoID
GROUP BY 
    p.Description

-----------------------------------------------------------------------------
CREATE VIEW VistaRotacionInventario AS
WITH InventarioPromedio AS (
    SELECT 
        v.ProductoID,
        (AVG(i.onHand) + AVG(f.onHand)) / 2 AS InvPromedio
    FROM 
        Fact_Ventas v
    JOIN 
        Fact_InvInicio i ON v.ProductoID = i.ProductoID
    JOIN 
        Fact_InvFinYear f ON v.ProductoID = f.ProductoID
    GROUP BY 
        v.ProductoID
)
SELECT 
    p.Description AS Producto,
    SUM(v.SalesDollars) AS COGS,  -- Suma de las ventas en dólares (Costo de los bienes vendidos)
    CASE 
        WHEN ip.InvPromedio IS NOT NULL AND ip.InvPromedio > 0 THEN
            (SUM(v.SalesDollars) / ip.InvPromedio) * 100  -- Inventario promedio como porcentaje
        ELSE
            0  -- Evitar división por cero
    END AS InventarioPromedioPorcentaje,
    CASE
        WHEN ip.InvPromedio > 0 THEN
            CAST(SUM(v.SalesDollars) / ip.InvPromedio AS DECIMAL(10, 2))  -- Cálculo de rotación de inventario con 2 decimales
        ELSE
            0  -- Evitar división por cero
    END AS RotacionInventario
FROM 
    Fact_Ventas v
JOIN 
    Dim_Producto p ON v.ProductoID = p.ProductoID
JOIN 
    InventarioPromedio ip ON v.ProductoID = ip.ProductoID  -- Usar el resultado del CTE (InventarioPromedio)
GROUP BY 
    p.Description, ip.InvPromedio;

------------------------------------------------------------------------------------------
CREATE VIEW TasaDiasInventario AS
WITH InventarioPromedio AS (
    SELECT 
        v.ProductoID,
        (AVG(i.onHand) + AVG(f.onHand)) / 2 AS InvPromedio
    FROM 
        Fact_Ventas v
    JOIN 
        Fact_InvInicio i ON v.ProductoID = i.ProductoID
    JOIN 
        Fact_InvFinYear f ON v.ProductoID = f.ProductoID
    GROUP BY 
        v.ProductoID
),
CostoVentas AS (
    SELECT 
        v.ProductoID,
        SUM(v.SalesDollars) AS COGS,
        COUNT(DISTINCT v.SalesDate) AS Dias
    FROM 
        Fact_Ventas v
    WHERE 
        v.SalesDate BETWEEN '2016-01-01' AND '2016-12-31'  -- Ajusta las fechas según tu necesidad
    GROUP BY 
        v.ProductoID
)
SELECT 
    p.Description AS Producto,
    CASE 
        WHEN ip.InvPromedio > 0 AND c.COGS > 0 THEN 
            CAST(ip.InvPromedio / (c.COGS / c.Dias) AS DECIMAL(10, 2))  -- Días de inventario
        ELSE 
            0
    END AS DiasInventario
FROM 
    Dim_Producto p
JOIN 
    InventarioPromedio ip ON p.ProductoID = ip.ProductoID
JOIN 
    CostoVentas c ON p.ProductoID = c.ProductoID;

-------------------------------------------------------------------


CREATE VIEW VistaCreditoOtorgado AS
SELECT 
    v.VendorID,
    v.VendorName,
    FORMAT(SUM(c.Dollars), 'N2') AS TotalComprado,
    FORMAT(SUM(c.Dollars) * 0.8, 'N2') AS CreditoOtorgado,  -- 80% del total comprado
    SUM(c.Quantity) AS TotalCantidadComprada  -- Suma de la cantidad de productos comprados
FROM 
    Fact_Compras c
JOIN 
    Dim_Producto p ON c.ProductoID = p.ProductoID  -- Une Fact_Compras con DimProducto
JOIN 
    DimVendor v ON p.VendorID = v.VendorID  -- Une DimProducto con DimVendor
GROUP BY 
    v.VendorID, v.VendorName;

	SELECT*FROM VistaCreditoOtorgado
---------------------------------------------------------------------------------
CREATE VIEW top5VistaCreditoOtorgado AS
SELECT top 5
    v.VendorID,
    v.VendorName,
    FORMAT(SUM(c.Dollars), 'N2') AS TotalComprado,
    FORMAT(SUM(c.Dollars) * 0.8, 'N2') AS CreditoOtorgado  -- 80% del total comprado
FROM 
    Fact_Compras c
JOIN 
    Dim_Producto p ON c.ProductoID = p.ProductoID  -- Une Fact_Compras con DimProducto
JOIN 
    DimVendor v ON p.VendorID = v.VendorID  -- Une DimProducto con DimVendor
GROUP BY 
    v.VendorID, v.VendorName;

	select*from top5VistaCreditoOtorgado
----------------------------------------------------------------------------------------------------
CREATE VIEW VistaTop10Proveedores AS
SELECT TOP 10
    v.VendorID,
    v.VendorName,
    FORMAT(SUM(c.Dollars), 'N2') AS TotalComprado  -- Suma total de compras por proveedor
FROM 
    Fact_Compras c
JOIN 
    Dim_Producto p ON c.ProductoID = p.ProductoID  -- Une Fact_Compras con DimProducto
JOIN 
    DimVendor v ON p.VendorID = v.VendorID  -- Une DimProducto con DimVendor
GROUP BY 
    v.VendorID, v.VendorName;
SELECT*FROM VistaTop10Proveedores; 