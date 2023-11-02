USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B8_Table_Acte_Date_J0]    Script Date: 01/11/2023 18:14:55 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Preproc_B8_Table_Acte_Date_J0]  @TABLE_NIP_J0 nvarchar(50), @TABLE_ACT nvarchar(50), @TABLE_SORTIE as nvarchar(50) , @DATEREF as DATE
AS
SET NOCOUNT ON;

DECLARE
--@TABLE_ACT as nvarchar(50),
--@TABLE_UF as nvarchar(50),
--@TABLE_SORTIE_A as nvarchar(50),
@Query as nvarchar(max)


EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_SORTIE;

SET @Query='SELECT 
	T_Actes.ID_A as ID_A
	,T_Actes.DD_A as DD_A
	,T_NIP_J0.J0_V1 as J0_V1
	,DATEDIFF(D,T_NIP_J0.J0_V1,T_Actes.DD_A) as MyCarePath_V1_Day
	,DATEADD(D,DATEDIFF(D,T_NIP_J0.J0_V1,T_Actes.DD_A),'''+ CONVERT(nvarchar(20),@DATEREF,120)  + ''') as My_CP_Date_V1
	,T_NIP_J0.J0_V2 as J0_V2
	,DATEDIFF(D,T_NIP_J0.J0_V2,T_Actes.DD_A) as MyCarePath_V2_Day
	,DATEADD(D,DATEDIFF(D,T_NIP_J0.J0_V2,T_Actes.DD_A),'''+ CONVERT(nvarchar(20),@DATEREF,120)  + ''') as My_CP_Date_V2
	,T_NIP_J0.J0_V3 as J0_V3
	,DATEDIFF(D,T_NIP_J0.J0_V3,T_Actes.DD_A) as MyCarePath_V3_Day
	,DATEADD(D,DATEDIFF(D,T_NIP_J0.J0_V3,T_Actes.DD_A),'''+ CONVERT(nvarchar(20),@DATEREF,120)  + ''') as My_CP_Date_V3
	,T_NIP_J0.J0_V4 as J0_V4
	,DATEDIFF(D,T_NIP_J0.J0_V4,T_Actes.DD_A) as MyCarePath_V4_Day
	,DATEADD(D,DATEDIFF(D,T_NIP_J0.J0_V4,T_Actes.DD_A),'''+ CONVERT(nvarchar(20),@DATEREF,120)  + ''') as My_CP_Date_V4

'
SET @Query=@Query + '
INTO ' + @TABLE_SORTIE + ' 
FROM 
	[ICO_Activite].[dbo].[' + @TABLE_ACT + '] as T_Actes,
	[ICO_Activite].[dbo].[' + @TABLE_NIP_J0 + '] as T_NIP_J0
	
WHERE T_Actes.NIP=T_NIP_J0.NIP
'
PRINT('[Preproc_B8_Table_Acte_Date_J0]  - ' + @Query) 
EXEC(@Query);
