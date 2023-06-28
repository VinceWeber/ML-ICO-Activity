/****** Script for SelectTopNRows command from SSMS  ******/

/*
Date début et fin de séjour
	Données d'entrée = A_Actes-ICO-2018-2021_Global0

	Processus = 
		Date de début de séjour = la date du 1er acte comme date de début de séjour
		Date de fin de séjour = la date du dernier acte du séjour

	Données de sortie = AAA_Tmp3_Ddetfinsejour

*/
DROP TABLE AAA_Tmp3_Ddetfinsejour
SELECT        Ho_Num_Num_sejour, MIN(Date_début_acte) AS Ddebsej, MAX([Date-Fin_Mvt]) AS Dfinsej
INTO AAA_Tmp3_Ddetfinsejour
FROM            [A_Actes-ICO-2018-2021_Global0]
GROUP BY Ho_Num_Num_sejour