/*
Sortie intermédiaire pour traitement des données
	Données d'entrée = 
		A_Actes_ICO_2018_2021
		AAA_Tmp3_Ddetfinsejour
		AAA_Tmp2_J0V12
		AAA_Tmp_Poids_Sejours2

	Processus = 
		Formate une sortie d'un jeu de donnée pour lancer une routine de définition des séquences de parcours "INIT, TRAIT, SUVI_CT, SUIVI_LT"

	Données de sortie = 
		AAA_Tmp5_Export_Pour_Trait_Donnée
*/

CREATE PROCEDURE dbo.Preproc_A4 @TABLE_ACT nvarchar(50), @TABLE_UF nvarchar(50), @TABLE_SORTIE nvarchar(50), @TABLE_Poids_Sejours nvarchar(50),@TABLE_J0V1V2 nvarchar(50),
								@TABLE_Ddetfinsejour nvarchar(50)

AS
SET NOCOUNT ON;


DECLARE
--@TABLE_UF AS nvarchar(50),
--@TABLE_ACT as nvarchar(50),
--@TABLE_Ddetfinsejour as nvarchar(50),
--@TABLE_Poids_Sejours as nvarchar(50), 
--@TABLE_J0V1V2 as nvarchar(50),
--@TABLE_SORTIE as nvarchar(50),
--@TABLE_SORTIE2 as nvarchar(50),
--@Seuil as BIGINT,
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50),


@Query as nvarchar(2000)
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50)

--SET @TABLE_UF='A_Liste_UF_V3'
--SET @TABLE_UF='A_Listing_UF' 
--SET @TABLE_ACT='A_Actes_ICO_2018_2021'
--SET @TABLE_Ddetfinsejour='AAA_Tmp3_Ddetfinsejour'
--SET @TABLE_J0V1V2='AAA_Tmp2_J0V12'
--SET @TABLE_Poids_Sejours='AAA_Tmp_Poids_Sejours2'
--SET @TABLE_SORTIE1='AAA_Tmp5_Export_Pour_Trait_Donnée'
--SET @TABLE_SORTIE2=''

SET @Query=''

PRINT '**********---STARTING A4 - PROCEDURE APPLY POIDS ACTES WITH INPUT PARAMETERS @TABLE_ACT : //' + @TABLE_ACT + '// AND @TABLE_UF : //' + @TABLE_UF + '// 
		AND @SORTIE : //' + @TABLE_SORTIE + '// AND @TABLE_Poids_Sejours : //' + @TABLE_Poids_Sejours + '// AND @TABLE_J0V1V2 : //' + @TABLE_J0V1V2 + '// 
		AND @TABLE_Ddetfinsejour : //' + @TABLE_Ddetfinsejour 

EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_SORTIE

--IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AAA_Tmp5_Export_Pour_Trait_Donnée]') AND type in (N'U'))
--DROP TABLE AAA_Tmp5_Export_Pour_Trait_Donnée


SET @Query= @Query + ' SELECT [' + @TABLE_ACT +'].NIP_original, [' + @TABLE_ACT +'].Ho_Num_Num_sejour, ' + @TABLE_Ddetfinsejour +'.Ddebsej, ' + @TABLE_J0V1V2 +'.J0_V1, ' + @TABLE_J0V1V2 + '.J0_V2, 
	SUBSTRING ([' + @TABLE_ACT +'].NIP_original,2,4) as Annee_NIP, ' + @TABLE_Poids_Sejours +'.Poids_Sejour_DS, ' + @TABLE_Poids_Sejours + '.Poids_Sejour_DSOS 
	'
SET @Query= @Query + 'INTO ' + @TABLE_SORTIE +'
'
SET @Query= @Query + 'FROM [' + @TABLE_ACT +'] INNER JOIN 
                         ' + @TABLE_Ddetfinsejour +' ON ['+ @TABLE_ACT +'].Ho_Num_Num_sejour = ' + @TABLE_Ddetfinsejour +'.Ho_Num_Num_sejour INNER JOIN
                         ' + @TABLE_J0V1V2 +' ON [' + @TABLE_ACT +'].NIP_original = '+@TABLE_J0V1V2 +'.NIP_original INNER JOIN
                         ' + @TABLE_Poids_Sejours +' ON [' + @TABLE_ACT +'].Ho_Num_Num_sejour = ' + @TABLE_Poids_Sejours +'.Ho_Num_Num_sejour
						 '
SET @Query= @Query + 'GROUP BY [' + @TABLE_ACT + '].NIP_original, [' + @TABLE_ACT +'].Ho_Num_Num_sejour, '+@TABLE_Ddetfinsejour +'.Ddebsej, ' + @TABLE_J0V1V2 +'.J0_V1, ' + @TABLE_J0V1V2 +'.J0_V2, 
                          ' + @TABLE_Poids_Sejours +'.Poids_Sejour_DS, ' + @TABLE_Poids_Sejours + '.Poids_Sejour_DSOS
						  '
SET @Query= @Query + 'ORDER BY NIP_original, Ho_Num_Num_sejour'


PRINT '*****---QUERY OUTPUT FROM A4 ON TABLE ' + @TABLE_SORTIE + ':
' + @Query
EXEC (@Query);