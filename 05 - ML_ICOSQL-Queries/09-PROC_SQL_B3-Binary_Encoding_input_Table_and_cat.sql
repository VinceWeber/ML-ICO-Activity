/****** Script for SelectTopNRows command from SSMS  ******/


--VARIABLES

/*
	PROCEDURE :
		INPUT PARAMETER : TABLE CATEGORIE
		OUPUT PARAMETER : TABLE ENCODEE
*/


CREATE PROCEDURE [dbo].[Preproc_B3_Binary_Encoding] @Table_Cat nvarchar(50), @Table_Sortie nvarchar(50), @Cat nvarchar(50)

AS
SET NOCOUNT ON;

DECLARE
@MaxRow int,
@BaseLenght int,
@NBofbits int,
--@Table_Cat nvarchar(50),
--@Table_Sortie nvarchar(50),
@Query nvarchar(max)

--DEFINIR LES VALEURS DES VARIABLES :
	--Max row
	--LEN(Binary(Max(Row))
	--UTILISER LA FONCTION LPAD() POUR AJOUTER DES ZEROS DEVANT LA CHAINE ENCODEE

--SET @Table_Cat='TABLE_TEST_BINARY_ENCODING'
--SET @Table_Sortie='Tmp_TABLE_SORTIE'

EXECUTE Delete_Table_if_exists Tmp_Chk_row
EXECUTE Delete_Table_if_exists @Table_Sortie

SET @Query='SELECT DISTINCT (' + @Cat + ') INTO Tmp_Chk_row FROM ' + @Table_Cat 
EXEC(@Query)
SET @MaxRow = (SELECT COUNT(*) FROM dbo.Tmp_Chk_row)

SET @NBofbits=4

PRINT('MaxRow : ' + CAST(@MaxRow as varchar))
IF @MaxRow > POWER(CONVERT(BIGINT,2), 8*@NBofbits) 
	BEGIN
		THROW 51000, 'Not enought memory for binary encoding, set a bigger value in Binary Encoding Procedure',1;
	END
ELSE
	BEGIN
		PRINT('Ok, MaxRow = ' + CAST(@Maxrow as nvarchar(20)) + ' encoding on ' + CAST(@Nbofbits as nvarchar(5)) + 'bits')
	END

SET @Query=' SELECT 
	--ROW_NUMBER() OVER(ORDER BY ' + @Cat + ' ASC) AS Row#,
	' + @Cat + ',
	--[dbo].[ConvertToBase]('+ @Cat +',2) as Cle_Binary_Encoded,
	--[dbo].[ConvertToBase](ROW_NUMBER() OVER(ORDER BY '+ @Cat +' ASC),2) AS Row_Encoded#,
	--CAST( [dbo].[ConvertToBase](ROW_NUMBER() OVER(ORDER BY '+ @Cat +' ASC)),2) as nvarchar(20)) AS Row_Encoded2#
	CAST(ROW_NUMBER() OVER(ORDER BY '+ @Cat +' ASC) as binary(4)) AS Row_Encoded
  INTO ' + @Table_Sortie + '
  FROM dbo.Tmp_Chk_row'

--PRINT(@Query)
EXEC(@Query)
EXECUTE Delete_Table_if_exists Tmp_Chk_row
