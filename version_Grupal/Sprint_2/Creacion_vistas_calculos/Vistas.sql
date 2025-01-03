
-- Insight: Esta vista te muestra las ventas totales por tienda y por producto, útil para identificar qué productos tienen mejor rendimiento en cada tienda.
CREATE VIEW vw_VentasPorTiendaProducto AS
SELECT 
    F.Store,
    F.ProductoID,
    P.Description AS Producto,
    SUM(F.SalesQuantity) AS CantidadVendida,
    SUM(F.SalesDollars) AS VentasTotales
FROM 
    Fact_Ventas F
JOIN 
    Dim_Producto P ON F.ProductoID = P.ProductoID
GROUP BY 
    F.Store, F.ProductoID, P.Description;

SELECT * FROM vw_VentasPorTiendaProducto;
drop view  vw_VentasPorTiendaProducto;
-------------------------------------------------------------------------
/* Insight: Esta vista compara el inventario inicial y final por tienda y producto. */
select*
from vw_VentasPorTiendaProducto;

CREATE VIEW vw_InventarioInicialFinal AS
SELECT 
    I.Store,
    I.ProductoID,
    P.Description AS ProductoDescripcion,
    SUM(I.onHand) AS InventarioInicial,
    SUM(F.onHand) AS InventarioFinal
FROM Fact_InvInicio I
JOIN Fact_InvFinYear F ON I.Store = F.Store AND I.ProductoID = F.ProductoID
JOIN Dim_Producto P ON I.ProductoID = P.ProductoID
GROUP BY I.Store, I.ProductoID, P.Description;

-----------------------------------------------------------------------------
/* Insight: Esta vista compara las cantidades compradas y vendidas por producto. */
CREATE VIEW vw_ComprasVentasPorProducto AS
SELECT 
    P.ProductoID,
    P.Description AS ProductoDescripcion,
    COALESCE(SUM(C.Quantity), 0) AS TotalComprado,
    COALESCE(SUM(F.SalesQuantity), 0) AS TotalVendido
FROM Dim_Producto P
LEFT JOIN Fact_Compras C ON P.ProductoID = C.ProductoID
LEFT JOIN Fact_Ventas F ON P.ProductoID = F.ProductoID
GROUP BY P.ProductoID, P.Description;
---------------------------------------------------------------------------------
/* Insight: Esta vista calcula el margen de ganancia por producto (ventas menos compras). */
CREATE VIEW vw_MargenGananciaPorProducto AS
SELECT 
    P.ProductoID,
    P.Description AS ProductoDescripcion,
    SUM(F.SalesDollars) AS TotalVentas,
    SUM(C.Dollars) AS TotalCompras,
    (SUM(F.SalesDollars) - SUM(C.Dollars)) AS MargenGanancia
FROM Dim_Producto P
LEFT JOIN Fact_Compras C ON P.ProductoID = C.ProductoID
LEFT JOIN Fact_Ventas F ON P.ProductoID = F.ProductoID
GROUP BY P.ProductoID, P.Description;
-------------------------------------------------------------------------------------
/* Insight: Esta vista muestra el total de impuestos cobrados por producto. */
CREATE VIEW vw_ImpuestosPorProducto AS
SELECT 
    P.ProductoID,
    P.Description AS ProductoDescripcion,
    SUM(F.ExciseTax) AS TotalImpuestos
FROM Fact_Ventas F
JOIN Dim_Producto P ON F.ProductoID = P.ProductoID
GROUP BY P.ProductoID, P.Description;






