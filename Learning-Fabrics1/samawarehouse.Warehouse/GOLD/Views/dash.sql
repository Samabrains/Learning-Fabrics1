-- Auto Generated (Do not modify) 3B93B05EB88F0F6EFAD25D403318A85D7DDBA794E7CAD53099DEF268AE252658
CREATE VIEW [GOLD].[dash] AS (select [_].[ProductSubcategoryKey] as [ProductSubcategoryKey],
    lower([_].[SubcategoryName]) as [SubcategoryName],
    [_].[ProductCategoryKey] as [ProductCategoryKey],
    [_].[ProductCategoryKey.1] as [ProductCategoryKey.1],
    [_].[CategoryName] as [CategoryName]
from [samawarehouse].[GOLD].[Customers] as [_])