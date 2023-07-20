/****** Script for SelectTopNRows command from SSMS  ******/

/*
J0V12
	Donn�es d'entr�e = A_Actes-ICO-2018-2021_Global0

	Processus = D�fini un J0 de parcours dans 2 versions
				V1 = Date du 1er acte du jeu de donn�e
				V2 = Date du 1er acte qualifi� de "soins/traitement" (poids de l'acte = 100000000000) du jeu de donn�e
	Donn�es de sortie = AAA_Tmp2_J0V12

*/

/* VARIABLES D'ENTREE */

USE [ICO_Activite]
GO

CREATE PROCEDURE dbo.Preproc_A2 @TABLE_ACT nvarchar(50), @TABLE_UF nvarchar(50), @TABLE_SORTIE nvarchar(50)

AS
SET NOCOUNT ON;


--TABLE DES UF = 'A_Liste_UF_V3'
DECLARE
--@TABLE_UF AS nvarchar(50),
--@TABLE_ACT as nvarchar(50),
@TABLE_INTERM as nvarchar(50),
--@TABLE_SORTIE as nvarchar(50),
@Seuil as BIGINT,


@Query as nvarchar(2000)
--@Dim_Parcours as nvarchar(50),
--@Dim_Parcours2 as nvarchar(50)

--SET @TABLE_UF='A_Listing_UF' 
--SET @TABLE_UF='A_Liste_UF_V3' 
--SET @TABLE_ACT='A_Actes_ICO_2018_2021'
SET @TABLE_INTERM='AAA_Tmp_J0V12'
--SET @TABLE_SORTIE='AAA_Tmp2_J0V12'
SET @Seuil=100000000000

SET @Query=''

PRINT '**********---STARTING A2 - PROCEDURE APPLY POIDS ACTES WITH INPUT PARAMETERS @TABLE_ACT : //' + @TABLE_ACT + '// AND @TABLE_UF : //' + @TABLE_UF + '// AND @SORTIE : //' + @TABLE_SORTIE
EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_SORTIE
EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_INTERM

SET @Query= @Query + ' 
	SELECT [NIP_original]
		  ,[Date_d�but_acte] as J0_V1
		  ,CONVERT(VARCHAR,''12-31-2099 00:00:00'',103) as J0_V2
    INTO ' + @TABLE_INTERM + '
	FROM ' + @TABLE_ACT 
	
SET @Query= @Query	+ ' UNION ' 
 
SET @Query= @Query	+ '

	SELECT        ' + @TABLE_ACT + '.NIP_original, CONVERT(VARCHAR,''12-31-2099 00:00:00'',103) as J0_V1, '+ @TABLE_ACT +'.Date_d�but_acte AS J0_V2
	FROM          ' + @TABLE_ACT + ' INNER JOIN
							' + @TABLE_UF + ' ON ' + @TABLE_ACT + '.UFX_UFX_Code = ' + @TABLE_UF + '.UFX_Code
	WHERE ' + @TABLE_UF + '.[Poids_acte]= ' + CAST(@Seuil as nvarchar(20))

PRINT '*****---QUERY : 
' + @Query
EXEC (@Query);

	/* concatenation */
SET @Query= '	
	SELECT [NIP_original]
      ,min([J0_V1]) as J0_V1
      ,min([J0_V2]) as J0_V2
  INTO '
SET @Query= @Query	+  @TABLE_SORTIE 
SET @Query= @Query	+ ' FROM '
SET @Query= @Query	+  @TABLE_INTERM
SET @Query= @Query	+ ' GROUP BY  [NIP_original]'

PRINT '*****---QUERY OUTPUT FROM A2 ON TABLE : ' + @TABLE_SORTIE + ' 
' + @Query
EXEC (@Query);

EXECUTE [dbo].[Delete_Table_if_exists] @TABLE_INTERM