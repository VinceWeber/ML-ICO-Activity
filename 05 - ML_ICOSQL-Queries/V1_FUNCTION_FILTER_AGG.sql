DECLARE @Text nvarchar(50)

SET @Text=dbo.F_Filter_Aggreg('1','FILTER VALUE','FOM')

PRINT @Text