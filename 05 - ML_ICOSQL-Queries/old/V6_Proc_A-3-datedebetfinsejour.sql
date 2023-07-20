/****** Script for SelectTopNRows command from SSMS  ******/

/*
Date début et fin de séjour
	Données d'entrée = A_Actes-ICO-2018-2021_Global0

	Processus = 
		Date de début de séjour = la date du 1er acte comme date de début de séjour
		Date de fin de séjour = la date du dernier acte du séjour

	Données de sortie = AAA_Tmp3_Ddetfinsejour

*/

CREATE PROCEDURE dbo.Preproc_A3 @TABLE_ACT nvarchar(50), @TABLE_UF nvarchar(50), @TABLE_SORTIE nvarchar(50), @Seuil as BIGINT

AS
SET NOCOUNT ON;


DECLARE
--@TABLE_ACT as nvarchar(50),
--@TABLE_SORTIE as nvarchar(50),
--@Seuil as BIGINT,


@Query as nvarchar(2000)
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50)

--SET @TABLE_UF='A_Liste_UF_V3' 
--SET @TABLE_ACT='A_Actes_ICO_2018_2021'
--SET @TABLE_SORTIE='AAA_Tmp3_Ddetfinsejour'



PRINT '**********---STARTING A3 - PROCEDURE APPLY POIDS ACTES WITH INPUT PARAMETERS @TABLE_ACT : //' + @TABLE_ACT + '// AND @TABLE_UF : //' + @TABLE_UF + '// AND @SORTIE : //' + @TABLE_SORTIE+ '// AND @SEUIL : //' + CAST(@SEUIL as nvarchar(50))

PRINT 'SEUIL DEFINED BY USER AT ' + CAST(@SEUIL as nvarchar(50))
SET @Seuil=100000000000
PRINT 'SET SEUIL AT ' + CAST(@SEUIL as nvarchar(50))


SET @Query=''

EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_SORTIE


SET @Query= @Query +' SELECT N_S, MIN(DD_A) AS Ddebsej, MAX([DF_M]) AS Dfinsej '
SET @Query= @Query +' INTO ' + @TABLE_SORTIE
SET @Query= @Query +' FROM ' +@TABLE_ACT
SET @Query= @Query +' GROUP BY N_S'

PRINT '*****---QUERY OUTPUT FROM A3 ON TABLE :' + @TABLE_SORTIE + '
' + @Query
EXEC (@Query);