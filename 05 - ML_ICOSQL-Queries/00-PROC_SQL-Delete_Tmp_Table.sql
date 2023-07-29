CREATE PROCEDURE dbo.Delete_TmpTables
AS
BEGIN
    DECLARE @tableName NVARCHAR(128)
    DECLARE tableCursor CURSOR FOR
    SELECT name
    FROM sys.tables
    WHERE name LIKE 'Tmp_%'

    OPEN tableCursor
    FETCH NEXT FROM tableCursor INTO @tableName

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @sql NVARCHAR(MAX)
        SET @sql = N'DROP TABLE ' + QUOTENAME(@tableName)
        EXEC sp_executesql @sql
        FETCH NEXT FROM tableCursor INTO @tableName
    END

    CLOSE tableCursor
    DEALLOCATE tableCursor
END