USE brindis_real 

--- productos cantidad dolar *-----
CREATE VIEW Top5ProductosPorCantidadYDollars AS
SELECT TOP 5
    fc.ProductoID,
    dp.Classification AS Clasificacion,
    SUM(fc.Quantity) AS Cantidad,  -- Sumar las cantidades
    SUM(fc.Dollars) AS TotalDollars  -- Sumar los d�lares
FROM [brindis_real].[dbo].[Fact_Compras] fc
JOIN [brindis_real].[dbo].[Dim_Producto] dp
    ON fc.ProductoID = dp.ProductoID
GROUP BY fc.ProductoID, dp.Classification  -- Agrupar por ProductoID y clasificaci�n
ORDER BY Cantidad DESC, TotalDollars DESC;  -- Ordenar por cantidad y total en d�lares



--- valor inventario en dolares -----

CREATE VIEW ValorTotalInventarioEnDolares AS
SELECT 
    SUM(fi.onHand * dp.PurchasePrice) AS ValorTotalInventarioDolares
FROM [brindis_real].[dbo].[Fact_InvFinYear] fi
JOIN [brindis_real].[dbo].[Dim_Producto] dp
    ON fi.ProductoID = dp.ProductoID;


--- valor promedio inventario ---
CREATE VIEW ValorPromedioInventarioEnDolares AS
SELECT 
    AVG(fi.onHand * dp.PurchasePrice) AS ValorPromedioInventarioDolares
FROM [brindis_real].[dbo].[Fact_InvFinYear] fi
JOIN [brindis_real].[dbo].[Dim_Producto] dp
    ON fi.ProductoID = dp.ProductoID
WHERE fi.onHand > 0;  -- Solo incluir productos que tienen inventario

-------------------------------------------------------------------
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
    SUM(v.SalesDollars) AS COGS,  -- Suma de las ventas en d�lares (Costo de los bienes vendidos)
    ip.InvPromedio AS InventarioPromedio,  -- Inventario Promedio calculado previamente
    CASE
        WHEN ip.InvPromedio > 0 THEN
            SUM(v.SalesDollars) / ip.InvPromedio  -- C�lculo de rotaci�n de inventario
        ELSE
            0  -- Evitar divisi�n por cero
    END AS RotacionInventario
FROM 
    Fact_Ventas v
JOIN 
    Dim_Producto p ON v.ProductoID = p.ProductoID
JOIN 
    InventarioPromedio ip ON v.ProductoID = ip.ProductoID  -- Usar el resultado del CTE (InventarioPromedio)
GROUP BY 
    p.Description, ip.InvPromedio;  -- Agrupar por descripci�n y promedio de inventario

------------------------------------------------------------------------------------------------
CREATE VIEW ComparacionInventario AS
WITH InventarioActual AS (
    SELECT 
        SUM(fi.onHand * dp.PurchasePrice) AS ValorInventarioActualDolares
    FROM [brindis_real].[dbo].[Fact_InvFinYear] fi
    JOIN [brindis_real].[dbo].[Dim_Producto] dp
        ON fi.ProductoID = dp.ProductoID
),
InventarioInicial AS (
    SELECT 
        SUM(ii.onHand * dp.PurchasePrice) AS ValorInventarioInicialDolares
    FROM [brindis_real].[dbo].[Fact_InvInicio] ii
    JOIN [brindis_real].[dbo].[Dim_Producto] dp
        ON ii.ProductoID = dp.ProductoID
)
SELECT 
    ia.ValorInventarioActualDolares,
    ii.ValorInventarioInicialDolares,
    ((ia.ValorInventarioActualDolares - ii.ValorInventarioInicialDolares) / ii.ValorInventarioInicialDolares) * 100 AS PorcentajeCambioInventario
FROM InventarioActual ia
CROSS JOIN InventarioInicial ii;

---------------total de productos ------
CREATE VIEW VistaTotalProductosInventario AS
SELECT 
    SUM(onHand) AS TotalProductosInventario
FROM 
    [brindis_real].[dbo].[Fact_InvFinYear];


-------------------------------------------------------

SELECT 
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
    v.VendorID,�v.VendorName;

--------------------------------------------------------------------
