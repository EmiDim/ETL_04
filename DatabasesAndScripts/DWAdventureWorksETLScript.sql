USE [DWAdventureWorks_Basics];
GO

-- ETL Functions
CREATE OR ALTER FUNCTION dbo.fMoneyToDecimal(@Data Money)
RETURNS Decimal(18,4)
BEGIN 
	RETURN Cast(@Data as decimal(18,4));
END;
GO

CREATE OR ALTER FUNCTION dbo.fReplaceIntNull(@inData int)
RETURNS int
BEGIN
	RETURN COALESCE(@inData,-1)
END;
GO

CREATE OR ALTER FUNCTION dbo.fReplaceStringNull(@inData nvarchar(max))
RETURNS nvarchar(max)
BEGIN
	RETURN COALESCE(@inData,'NA')
END;
GO

-- ETL Stored Procedures
CREATE OR ALTER PROCEDURE pETLDropConstraints
AS
BEGIN
   BEGIN TRY
	ALTER TABLE FactSalesOrders DROP CONSTRAINT FK_FactSalesOrders_DimCustomers;
	ALTER TABLE FactSalesOrders DROP CONSTRAINT FK_FactSalesOrders_DimDates;
	ALTER TABLE FactSalesOrders DROP CONSTRAINT FK_FactSalesOrders_DimProducts;
   END TRY
   BEGIN CATCH
	SELECT 
    ERROR_NUMBER() AS ErrorNumber,
    ERROR_SEVERITY() AS ErrorSeverity,
    ERROR_STATE() as ErrorState,
    ERROR_PROCEDURE() as ErrorProcedure,
    ERROR_LINE() as ErrorLine,
    ERROR_MESSAGE() as ErrorMessage;
   END CATCH
 END;
GO

CREATE OR ALTER PROCEDURE pETLCreateConstraints
AS
BEGIN
   BEGIN TRY
	ALTER TABLE [dbo].[FactSalesOrders] ADD CONSTRAINT [FK_FactSalesOrders_DimCustomers] 
		FOREIGN KEY ([CustomerKey]) REFERENCES [dbo].[DimCustomers] ([CustomerKey]);
	ALTER TABLE [dbo].[FactSalesOrders] ADD CONSTRAINT [FK_FactSalesOrders_DimProducts]
		FOREIGN KEY ([ProductKey]) REFERENCES [dbo].[DimProducts] ([ProductKey]);
	ALTER TABLE [dbo].[FactSalesOrders] ADD CONSTRAINT [FK_FactSalesOrders_DimDates]
		FOREIGN KEY ([OrderDateKey]) REFERENCES [dbo].[DimDates] ([DateKey]);
   END TRY
   BEGIN CATCH
	SELECT 
    ERROR_NUMBER() AS ErrorNumber,
    ERROR_SEVERITY() AS ErrorSeverity,
    ERROR_STATE() as ErrorState,
    ERROR_PROCEDURE() as ErrorProcedure,
    ERROR_LINE() as ErrorLine,
    ERROR_MESSAGE() as ErrorMessage;
   END CATCH
 END;
GO 

CREATE OR ALTER PROCEDURE pETLClearTables
AS
 BEGIN
   BEGIN TRY
    TRUNCATE TABLE FactSalesOrders;

	TRUNCATE TABLE DimProducts;
	TRUNCATE TABLE DimCustomers;
	TRUNCATE TABLE DimDates;
	
   END TRY
   BEGIN CATCH
	SELECT 
    ERROR_NUMBER() AS ErrorNumber,
    ERROR_SEVERITY() AS ErrorSeverity,
    ERROR_STATE() as ErrorState,
    ERROR_PROCEDURE() as ErrorProcedure,
    ERROR_LINE() as ErrorLine,
    ERROR_MESSAGE() as ErrorMessage;
   END CATCH
 END;
GO

CREATE OR ALTER PROCEDURE pETLFillDimDates
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --	  
      --Delete From DimDates; -- Clears table data with the need for dropping FKs
	  If ((Select Count(*) From DimDates) = 0)
	  Begin
		  Declare @StartDate datetime = '01/01/2000' --< NOTE THE DATE RANGE!
		  Declare @EndDate datetime = '12/31/2010' --< NOTE THE DATE RANGE! 
		  Declare @DateInProcess datetime  = @StartDate
		  -- Loop through the dates until you reach the end date
		  While @DateInProcess <= @EndDate
		   Begin
		   -- Add a row into the date dimension table for this date
		   Insert Into DimDates 
		   ( [DateKey], [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] )
		   Values ( 
			 Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
			,@DateInProcess -- [FullDate]
			,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [FullDateName]  
			,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthID]
			,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
			,Year(@DateInProcess) -- [YearID] 
			,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
			)  
		   -- Add a day and loop again
		   Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
		   End -- While
	   End -- If
   Set @RC = +1
  End Try
  Begin Catch
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

CREATE OR ALTER PROCEDURE pETLFillDimCustomers
AS
 BEGIN
   DECLARE @RC int=0
   BEGIN TRY 
	  INSERT INTO [DWAdventureWorks_Basics].[dbo].DimCustomers
	   (CustomerId, CustomerFullName, 
	   CustomerCityName, CustomerStateProvinceName, 
	   CustomerCountryRegionCode, CustomerCountryRegionName)
	  SELECT 
	[CustomerID]=C.CustomerID
,	[CustomerFullName]=Cast(CONCAT(C.FirstName,' ',C.LastName) as nvarchar(100))
,	[CustomerCityName]=Cast(C.City as nvarchar(50))
,	[CustomerStateProvinceName]=Cast(C.StateProvinceName as nvarchar(50))
,	[CustomerCountryRegionCode]=Cast(C.CountryRegionCode as nvarchar(50))
,	[CustomerCountryRegionName]=Cast(C.CountryRegionName as nvarchar(50))
FROM [AdventureWorks_Basics].dbo.Customer AS C;
    SET @RC+=1;
   END TRY
   BEGIN CATCH
    SET @RC=-1;
   END CATCH
   return @RC;
 END
;
GO

CREATE OR ALTER PROCEDURE pETLFillDimProducts
AS
 BEGIN
    DECLARE @RC int=0
   BEGIN TRY
	  INSERT INTO [DWAdventureWorks_Basics].[dbo].[DimProducts]
	   (ProductID, ProductName, StandardListPrice, 
		ProductSubcategoryID, ProductSubcategoryName, ProductCategoryID, ProductCategoryName)
	  SELECT 
	[ProductID]=P.ProductID
,	[ProductName]=Cast(P.Name as nvarchar(100))
,	[StandardListPrice]=dbo.fMoneyToDecimal(P.ListPrice)
,	[ProductSubcategoryID]=dbo.fReplaceIntNull(PSC.ProductSubcategoryID)
,	[ProductSubcategoryName]=Cast(dbo.fReplaceStringNull(PSC.Name) as nvarchar(50))
,	[ProductCategoryID]=dbo.fReplaceIntNull(PC.ProductCategoryID)
,	[ProductCategoryName]=Cast(dbo.fReplaceStringNull(PC.Name) as nvarchar(50))
FROM [AdventureWorks_Basics].dbo.Products AS P
	LEFT OUTER JOIN [AdventureWorks_Basics].dbo.ProductSubcategory AS PSC
		ON P.ProductSubcategoryID=PSC.ProductSubcategoryID
	LEFT OUTER JOIN [AdventureWorks_Basics].dbo.ProductCategory AS PC
		ON PSC.ProductCategoryID=PC.ProductCategoryID;
        SET @RC+=1;
   END TRY
   BEGIN CATCH
   	SET @RC=-1;
   END CATCH
   RETURN @RC;
 END
;
GO

CREATE OR ALTER PROCEDURE pETLFillFactSalesOrders
AS
 BEGIN
   DECLARE @RC int = 0;
   BEGIN TRY
	  INSERT INTO FactSalesOrders (SalesOrderID, SalesOrderDetailID, 
        OrderDateKey, CustomerKey, ProductKey, 
		OrderQty, ActualUnitPrice)
	  SELECT 
	        [SalesOrderID]=SOH.SalesOrderID
        ,	[SalesOrderDetailID]=SOD.SalesOrderDetailID
        ,	[OrderDateKey]=DD.DateKey
        ,	[CustomerKey]=DC.CustomerKey
        ,	[ProductKey]=DP.ProductKey
        ,	[OrderQty]=SOD.OrderQty
        ,	[ActualUnitPrice]=dbo.fMoneyToDecimal(SOD.UnitPrice)
      FROM [AdventureWorks_Basics].dbo.SalesOrderHeader AS SOH
	        INNER JOIN [AdventureWorks_Basics].dbo.SalesOrderDetail AS SOD
		        ON SOH.SalesOrderID=SOD.SalesOrderID
	        INNER JOIN DimDates AS DD 
		        ON SOH.OrderDate=DD.FullDate
	        INNER JOIN DimCustomers AS DC
		        ON SOH.CustomerID=DC.CustomerId
	        INNER JOIN DimProducts as DP
		        ON SOD.ProductID=DP.ProductID;
   SET @RC = +1
   END TRY
   BEGIN CATCH
    SET @RC = -1;
   END CATCH
  RETURN @RC;
 END
;
GO