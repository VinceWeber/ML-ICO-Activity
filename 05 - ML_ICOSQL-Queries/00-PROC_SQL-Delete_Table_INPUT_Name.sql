USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Delete_Table_if_exists]    Script Date: 11/07/2023 16:29:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[Delete_Table_if_exists] @TABLE_A_EFF nvarchar(50)
AS
SET NOCOUNT ON;


DECLARE
@Query as nvarchar(2000)
--@TABLE_A_EFF as nvarchar(50)
--SET  @TABLE_A_EFF='AAA_Tmp2_J0V12'

SET @Query=''

SET @QUERY=@QUERY + 'IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'
SET @QUERY=@QUERY + '''' + @TABLE_A_EFF +''''
SET @QUERY=@QUERY + ') AND type in (N''U''))
DROP TABLE ' +@TABLE_A_EFF

PRINT '---DELETE TABLE : ' + @TABLE_A_EFF +'  
' --+ @Query
EXEC (@Query);