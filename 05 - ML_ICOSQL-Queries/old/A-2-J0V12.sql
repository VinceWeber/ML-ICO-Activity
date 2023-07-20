/****** Script for SelectTopNRows command from SSMS  ******/

/*
J0V12
	Donn�es d'entr�e = A_Actes-ICO-2018-2021_Global0

	Processus = D�fini un J0 de parcours dans 2 versions
				V1 = Date du 1er acte du jeu de donn�e
				V2 = Date du 1er acte qualifi� de "soins/traitement" (poids de l'acte = 100000000000) du jeu de donn�e

	Donn�es de sortie = AAA_Tmp2_J0V12

*/


drop table AAA_Tmp2_J0V12

	SELECT [NIP_original]
		  ,[Date_d�but_acte] as J0_V1
		  ,'31-12-2099' as J0_V2
    INTO AAA_Tmp_J0V12
	FROM [ICOActivite].[dbo].[A_Actes-ICO-2018-2021_Global0]
 
UNION

	SELECT        [A_Actes-ICO-2018-2021_Global0].NIP_original, '31-12-2099' as J0_V1, [A_Actes-ICO-2018-2021_Global0].Date_d�but_acte AS J0_V2
	FROM          [A_Actes-ICO-2018-2021_Global0] INNER JOIN
							 A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
	WHERE A_Liste_UF_V3.[Poids de l'acte]=100000000000

	/* concatenation */
		
	SELECT [NIP_original]
      ,min([J0_V1]) as J0_V1
      ,min([J0_V2]) as J0_V2

  INTO AAA_Tmp2_J0V12 
  FROM AAA_Tmp_J0V12
  GROUP BY  [NIP_original]

  DROP TABLE AAA_Tmp_J0V12