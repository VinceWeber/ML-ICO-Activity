USE [ICO_Activite]
GO
/****** Object:  StoredProcedure [dbo].[PREPROC_B6_EXPORT_REGROUP_TABLES]    Script Date: 29/07/2023 12:29:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [dbo].[Preproc_B4_Prepare_Dataset_Encoding]    Script Date: 21/07/2023 17:23:24 ******/


CREATE PROCEDURE [dbo].[PREPROC_B6_EXPORT_REGROUP_TABLES] @Table_entree nvarchar(max),@Table_Sortie nvarchar(max),@DateDeb_Dataset DATETIME
AS
SET NOCOUNT ON;


DECLARE 
	--@Table_Sortie nvarchar(50),
	--@DateDeb_Dataset nvarchar(50) ,
	@Table nvarchar(50),
	@Datetext nvarchar(50)

SET @Datetext=CONVERT(nvarchar(max),@DateDeb_Dataset,120)

--SET @Table_Sortie='Tmp_Group_Carac_'
--SET	@DateDeb_Dataset='01-01-2019 00:00:00'


SET @Table=@Table_Sortie + 'Sejour'
EXECUTE Delete_Table_if_exists	@Table
SET @Table=@Table_Sortie + 'Sequence'
EXECUTE Delete_Table_if_exists	@Table
SET @Table=@Table_Sortie + 'Parcours'
EXECUTE Delete_Table_if_exists	@Table


DECLARE @cols AS NVARCHAR(MAX);
DECLARE @query AS NVARCHAR(MAX);


-- Construction de la liste des colonnes avec SUM()
DECLARE @sumCols AS NVARCHAR(MAX);
SET @sumCols = (
    SELECT STRING_AGG('SUM(' + QUOTENAME(column_name) + ') AS ' + QUOTENAME(column_name), ', ')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = @Table_entree
    AND (column_name LIKE 'Service_%' OR column_name LIKE 'Activite_%' OR column_name LIKE 'Phase_%' OR column_name LIKE 'Dimension_%')
);

-- AGGREGATION DES CARACTERISTIQUES PAR SEJOUR
SET @query = '
SELECT [NIP]
		,[id_Sequence]
		,[N_S]
		--,MIN([DD_A]) AS DD_
		--,MAX([DF_A]) as DF_ 
		,DATEDIFF(D,MIN(Table_Full.J0_V1),MIN(Table_Full.[DD_A])) as J_Parcours_V1
	    ,DATEDIFF(D,MIN(Table_Full.J0_V3),MIN(Table_Full.[DD_A])) as J_Parcours_V3
	    ,DATEDIFF(D,''' + @Datetext + ''',MIN(Table_Full.[DD_A])) as J_DataSet
	    ,DATEDIFF(D,MIN(Table_Full.[DD_A]),MAX(Table_Full.[DF_A]))+1 as Duree
		, ' + @sumCols + '

INTO ' + @Table_Sortie + 'Sejour' + '
FROM [ICO_Activite].[dbo].[' + @Table_entree +'] as Table_Full
GROUP BY [NIP], [id_Sequence],[N_S]
';
PRINT('SUM_ COL')
PRINT(@sumCols)
EXEC sp_executesql @query;

-- AGGREGATION DES CARACTERISTIQUES PAR SEQUENCE
SET @query = '
SELECT [NIP]
		,[id_Sequence]
		--,MIN([DD_A]) AS DD_
		--,MAX([DF_A]) as DF_ 
		,DATEDIFF(D,MIN(Table_Full.J0_V1),MIN(Table_Full.[DD_A])) as J_Parcours_V1
	    ,DATEDIFF(D,MIN(Table_Full.J0_V3),MIN(Table_Full.[DD_A])) as J_Parcours_V3
	    ,DATEDIFF(D,''' + @Datetext + ''',MIN(Table_Full.[DD_A])) as J_DataSet
	    ,DATEDIFF(D,MIN(Table_Full.[DD_A]),MAX(Table_Full.[DF_A]))+1 as Duree
		, ' + @sumCols + '

INTO ' + @Table_Sortie + 'Sequence' + '
FROM [ICO_Activite].[dbo].[' + @Table_entree +'] as Table_Full
GROUP BY [NIP], [id_Sequence]
';
--PRINT(@Query)
EXEC sp_executesql @query;

-- AGGREGATION DES CARACTERISTIQUES PAR PARCOURS PATIENT
SET @query = '
SELECT [NIP]
		--,MIN([DD_A]) AS DD_
		--,MAX([DF_A]) as DF_ 
		,DATEDIFF(D,MIN(Table_Full.J0_V1),MIN(Table_Full.[DD_A])) as J_Parcours_V1
	    ,DATEDIFF(D,MIN(Table_Full.J0_V3),MIN(Table_Full.[DD_A])) as J_Parcours_V3
	    ,DATEDIFF(D,''' + @Datetext + ''',MIN(Table_Full.[DD_A])) as J_DataSet
	    ,DATEDIFF(D,MIN(Table_Full.[DD_A]),MAX(Table_Full.[DF_A]))+1 as Duree
		, ' + @sumCols + '

INTO ' + @Table_Sortie + 'Parcours' + '
FROM [ICO_Activite].[dbo].[' + @Table_entree +'] as Table_Full
GROUP BY [NIP]
';
--PRINT(@Query)
EXEC sp_executesql @query;
GO
