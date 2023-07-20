/*
1 - Poids séjour
	Données d'entrée = A_Actes-ICO-2018-2021_Global0

	Processus = 
		1-Associe un "poids" de séjour à chaque n° de séjour
		2-Mentionne a quelle dimension de parcours est associé l'acte (Soins ou SOS).
				Le poids des actes du séjour est défini dans la table Listing_UF (Variable modifiable)

	Données de sortie = AAA_Tmp_Poids_Sejours2 (Variable modifiable)

*/
/* VARIABLES D'ENTREE */

USE [ICO_Activite]
GO

CREATE PROCEDURE dbo.Preproc_A1 @TABLE_ACT nvarchar(50), @TABLE_UF nvarchar(50), @TABLE_SORTIE nvarchar(50)

AS
SET NOCOUNT ON;


--TABLE DES UF = 'A_Liste_UF_V3'
DECLARE
--@TABLE_UF AS nvarchar(50),
--@TABLE_ACT as nvarchar(50),
--@TABLE_SORTIE as nvarchar(50),

@Query as nvarchar(2000),
@Dim_Parcours as nvarchar(50),
@Dim_Parcours2 as nvarchar(50)

--SET @TABLE_UF='A_Listing_UF' 
--SET @TABLE_UF='A_Liste_UF_V3' 
--SET @TABLE_ACT='A_Actes_ICO_2018_2021'
--SET @TABLE_SORTIE='AAA_Tmp_Poids_Sejours2'

SET @Query=''


PRINT '---STARTING PROCEDURE WITH INPUT PARAMETERS @TABLE_ACT : //' + @TABLE_ACT + '// AND @TABLE_UF : //' + @TABLE_UF + '//'

/* SUPRRESSION DES TABLES TEMPORAIRES*/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AAA_Tmp_Poids_Sejours2]') AND type in (N'U'))
DROP TABLE [dbo].[AAA_Tmp_Poids_Sejours2]

SET @Dim_Parcours='''Soins'''


SET @Query = @Query + ' ' + 'SELECT ' + @TABLE_ACT + '.Ho_Num_Num_sejour, sum(' + @TABLE_UF +'.[Poids_acte]) as Poids_Sejour_DS, sum(0) as Poids_Sejour_DSOS, ' + @TABLE_UF +'.[Dimension_Parcours]
' 
SET @Query = @Query + ' ' + 'INTO AAA_Tmp_Poids_Sejours 
'
SET @Query = @Query + ' ' + 'FROM ' + @TABLE_ACT +' INNER JOIN ' + @TABLE_UF +' ON ' + @TABLE_ACT +'.UFX_UFX_Code = ' + @TABLE_UF +'.UFX_Code
'
SET @Query = @Query + ' ' + 'WHERE ' + @TABLE_UF + '.[Dimension_Parcours]=' + @Dim_Parcours

SET @Query = @Query + ' ' + 'GROUP BY Ho_Num_Num_sejour, ' + @TABLE_UF +'.[Dimension_Parcours]
'
SET @Query = @Query + ' ' + 'UNION
'



SET @Dim_Parcours='''Soins Support'''

SET @Query = @Query + ' ' + 'SELECT ' + @TABLE_ACT + '.Ho_Num_Num_sejour, sum(' + @TABLE_UF +'.[Poids_acte]) as Poids_Sejour_DS, sum(0) as Poids_Sejour_DSOS, ' + @TABLE_UF +'.[Dimension_Parcours]
'
SET @Query = @Query + ' ' + 'FROM ' + @TABLE_ACT +' INNER JOIN ' + @TABLE_UF +' ON ' + @TABLE_ACT +'.UFX_UFX_Code = ' + @TABLE_UF +'.UFX_Code
'
SET @Query = @Query + ' ' + 'WHERE ' + @TABLE_UF + '.[Dimension_Parcours]=' + @Dim_Parcours

SET @Query = @Query + ' ' + 'GROUP BY Ho_Num_Num_sejour, ' + @TABLE_UF +'.[Dimension_Parcours]
'
SET @Query = @Query + ' ' + 'UNION
'

SET @Dim_Parcours='''NC'''
SET @Dim_Parcours2='''Hors_ICO'''

SET @Query = @Query + ' ' + 'SELECT ' + @TABLE_ACT + '.Ho_Num_Num_sejour, sum(' + @TABLE_UF +'.[Poids_acte]) as Poids_Sejour_DS, sum(0) as Poids_Sejour_DSOS, ' + @TABLE_UF +'.[Dimension_Parcours]
'
SET @Query = @Query + ' ' + 'FROM ' + @TABLE_ACT +' INNER JOIN ' + @TABLE_UF +' ON ' + @TABLE_ACT +'.UFX_UFX_Code = ' + @TABLE_UF +'.UFX_Code
'
SET @Query = @Query + ' ' + 'WHERE ' + @TABLE_UF + '.[Dimension_Parcours]=' + @Dim_Parcours +'or ' + @TABLE_UF + '.[Dimension_Parcours]=' + @Dim_Parcours2

SET @Query = @Query + ' ' + 'GROUP BY Ho_Num_Num_sejour, ' + @TABLE_UF +'.[Dimension_Parcours]
'
SET @Query = @Query + ' ' + 'ORDER BY Ho_Num_Num_sejour
'

PRINT '---QUERY : 
' + @Query
EXEC (@Query);



/* STEP 2  CONCATENATION POUR FINALISER LA TABLE N° SEJOUR + POIDS SEJOURS */

SET @Query = '' 
SET @Query = @Query + ' ' + 'SELECT [Ho_Num_Num_sejour]
							, max([Poids_Sejour_DS]) as Poids_Sejour_DS
							,max([Poids_Sejour_DSOS]) as Poids_Sejour_DSOS
							 INTO ' + @TABLE_SORTIE + ' 
							 FROM AAA_Tmp_Poids_Sejours
							 GROUP BY [Ho_Num_Num_sejour]
							 ORDER by [Ho_Num_Num_sejour]'
PRINT '---QUERY : 
' + @Query
EXEC (@Query);


  /* SUPRRESSION DES TABLES TEMPORAIRES*/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AAA_Tmp_Poids_Sejours]') AND type in (N'U'))
DROP TABLE [dbo].[AAA_Tmp_Poids_Sejours]

GO