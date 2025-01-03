-- Insight: Esta vista calcula el promedio de ventas por tienda y por mes, mostrando el mes en formato de texto.
CREATE VIEW vw_PromedioVentasMensual AS
SELECT 
    F.Store,
    DATENAME(MONTH, F.SalesDate) AS Mes,  -- Cambiado a nombre del mes en texto
    ROUND(AVG(F.SalesDollars), 2) AS PromedioVentasMensual
FROM Fact_Ventas F
GROUP BY F.Store, DATENAME(MONTH, F.SalesDate);

-- Para visualizar el resultado:
SELECT * FROM vw_PromedioVentasMensual;


--------------------------------------------------------------
--Prodcutos mas vendidos top 20
CREATE VIEW vw_Top20ProductosMasVendidos AS
SELECT TOP 20
    P.ProductoID,
    P.Description AS ProductoDescripcion,
    CAST(SUM(F.SalesQuantity) AS int ) AS CantidadVendida
FROM Fact_Ventas F
JOIN Dim_Producto P ON F.ProductoID = P.ProductoID
GROUP BY P.ProductoID, P.Description
ORDER BY SUM(F.SalesQuantity) DESC;

SELECT * FROM vw_Top20ProductosMasVendidos;

-------------------------------------------------------------------
-- muestra el costo total por orden de compra, incluyendo el costo de envío.
CREATE VIEW vw_CostoTotalPorOrdenCompra AS
SELECT 
    O.OrdenCompraID,
    O.VendorName,
    CAST(SUM(C.Dollars + O.Freight) AS int) CostoTotal
FROM Fact_Compras C
JOIN Dim_OC O ON C.OrdenCompraID = O.OrdenCompraID
GROUP BY O.OrdenCompraID, O.VendorName;

-- Para visualizar el resultado:
SELECT * FROM vw_CostoTotalPorOrdenCompra;

-----------------------------------------------------
-- Insight: Esta vista muestra el total de compras realizadas por cada tienda.
CREATE VIEW vw_ComprasPorTienda AS
SELECT 
    C.Store,
    CAST(SUM(C.Dollars) AS int ) TotalCompras
FROM Fact_Compras C
GROUP BY C.Store

-- Para visualizar el resultado:
SELECT * FROM vw_ComprasPorTienda;

-------------------------------------------------------


