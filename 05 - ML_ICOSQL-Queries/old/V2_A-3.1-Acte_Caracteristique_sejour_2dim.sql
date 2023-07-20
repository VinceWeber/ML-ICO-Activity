/*
Date début et fin de séjour
	Données d'entrée = 
		A_Actes-ICO-2018-2021_Global0
		A_Liste_UF_V3

	Processus = 
		Défini l'acte caractérisitque du séjour comme l'acte ayant le poids d'acte le pllus fort
		Calcul dans 2 dimensions : la dimension "Soins" et la dimension "SOS" (suivant le découpage dans les UF).

	Données de sortie = 
		AAA_Tmp4_Acte_Caracteristique_Soins
		AAA_Tmp4_Acte_Caracteristique_SOS
*/

DECLARE
@TABLE_UF AS nvarchar(50),
@TABLE_ACT as nvarchar(50),
@TABLE_INTERM as nvarchar(50),
@TABLE_INTERM2 as nvarchar(50),
@TABLE_SORTIE1 as nvarchar(50),
@TABLE_SORTIE2 as nvarchar(50),
@Seuil as BIGINT,
@Dim_Parcours as nvarchar(50),
@Dim_Parcours2 as nvarchar(50),


@Query as nvarchar(2000)
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50)

--SET @TABLE_UF='A_Liste_UF_V3'
SET @TABLE_UF='A_Listing_UF' 
SET @TABLE_ACT='A_Actes_ICO_2018_2021'
SET @TABLE_INTERM='AAA_Tmp3_Max_Dim_soins'
SET @TABLE_INTERM2='AAA_Tmp3_Max_Dim_SOS'
SET @TABLE_SORTIE1='AAA_Tmp4_Acte_Caracteristique_Soins'
SET @TABLE_SORTIE2='AAA_Tmp4_Acte_Caracteristique_SOS'

SET @Query=''


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AAA_Tmp4_Acte_Caracteristique_Soins]') AND type in (N'U'))
DROP TABLE AAA_Tmp4_Acte_Caracteristique_Soins
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AAA_Tmp4_Acte_Caracteristique_SOS]') AND type in (N'U'))
DROP TABLE AAA_Tmp4_Acte_Caracteristique_SOS

SET @Dim_Parcours='''Soins'''

SET @Query= @Query +' SELECT [' + @TABLE_ACT +' ].Ho_Num_Num_sejour,  max(' + @TABLE_UF +' .[Poids_acte]) as Max_Acte_Soins 
'
SET @Query= @Query +' INTO ' + @TABLE_INTERM
SET @Query= @Query +' FROM ' + @TABLE_ACT + ' INNER JOIN ' + @TABLE_UF + ' ON ' + @TABLE_ACT +' .UFX_UFX_Code = ' + @TABLE_UF + '.UFX_Code 
'
SET @Query= @Query +' WHERE ' + @TABLE_UF + '.[Dimension_Parcours] like ' + @Dim_Parcours 
SET @Query= @Query +' GROUP by Ho_Num_Num_sejour 
'
SET @Query= @Query +' Order by Ho_Num_Num_sejour'

PRINT '---QUERY : 
' + @Query
EXEC (@Query);


SET @Dim_Parcours='''Soins Sup%'''

SET @Query= ' SELECT [' + @TABLE_ACT + '].Ho_Num_Num_sejour,  max(' + @TABLE_UF +'.[Poids_acte]) as Max_Acte_SOS 
'
SET @Query= @Query +' INTO ' + @TABLE_INTERM2
SET @Query= @Query +' FROM [' + @TABLE_ACT +'] INNER JOIN ' + @TABLE_UF +' ON [' + @TABLE_ACT +'].UFX_UFX_Code = ' + @TABLE_UF + '.UFX_Code 
'
SET @Query= @Query +' WHERE ' + @TABLE_UF +'.[Dimension_Parcours] like ' + @Dim_Parcours
SET @Query= @Query +' GROUP by Ho_Num_Num_sejour
'
SET @Query= @Query +' Order by Ho_Num_Num_sejour'

PRINT '---QUERY : 
' + @Query
EXEC (@Query);

SET @Dim_Parcours='''Soins'''

SET @Query= ' SELECT min(cast([' + @TABLE_ACT +'].[idImport-Actes-CCAM] as float)) as Id_Acte_Caracteristique, [' + @TABLE_ACT +'].Ho_Num_Num_sejour 
'
SET @Query= @Query +' INTO ' + @TABLE_SORTIE1 + '
'
SET @Query= @Query +' FROM [' + @TABLE_ACT + '] INNER JOIN ' + @TABLE_INTERM +' ON [' + @TABLE_ACT +'].Ho_Num_Num_sejour = ' + @TABLE_INTERM + '.Ho_Num_Num_sejour INNER JOIN ' + @TABLE_UF + ' ON [' + @TABLE_ACT +'].UFX_UFX_Code = ' + @TABLE_UF + '.UFX_Code 
'
SET @Query= @Query +' WHERE ' + @TABLE_UF + '.[Dimension_Parcours] like ' + @Dim_Parcours +' and ' + @TABLE_UF + '.[Poids_acte]= '+@TABLE_INTERM+'.Max_Acte_Soins
'
SET @Query= @Query +' GROUP BY [' + @TABLE_ACT +'].Ho_Num_Num_sejour
'
SET @Query= @Query +' ORDER BY [' + @TABLE_ACT +'].Ho_Num_Num_sejour
'

PRINT '---QUERY : 
' + @Query
EXEC (@Query);




SET @Dim_Parcours='''Soins Sup%'''


SET @Query= ' SELECT min(cast([' + @TABLE_ACT +'].[idImport-Actes-CCAM] as float)) as Id_Acte_Caracteristique, [' + @TABLE_ACT + '].Ho_Num_Num_sejour
'
SET @Query= @Query +' INTO ' + @TABLE_SORTIE2
SET @Query= @Query +' FROM [' + @TABLE_ACT + '] 
INNER JOIN ' + @TABLE_INTERM2 + 'ON [' + @TABLE_ACT +'].Ho_Num_Num_sejour = ' + @TABLE_INTERM2 +'.Ho_Num_Num_sejour INNER JOIN' +
@TABLE_UF + 'ON [' + @TABLE_ACT +'].UFX_UFX_Code = ' + @TABLE_UF + '.UFX_Code
'
SET @Query= @Query +' WHERE ' + @TABLE_UF +'.[Dimension_Parcours] like ' + @Dim_Parcours + ' and A_Liste_UF_V3.[Poids_acte]=' + @TABLE_INTERM2 +'.Max_Acte_SOS 
'
SET @Query= @Query +' GROUP BY [' + @TABLE_ACT +'].Ho_Num_Num_sejour
'
SET @Query= @Query +' ORDER BY [' + @TABLE_ACT +'].Ho_Num_Num_sejour'


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AAA_Tmp3_Max_Dim_soins]') AND type in (N'U'))
DROP TABLE AAA_Tmp3_Max_Dim_soins
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AAA_Tmp3_Max_Dim_SOS]') AND type in (N'U'))
DROP TABLE AAA_Tmp3_Max_Dim_SOS
