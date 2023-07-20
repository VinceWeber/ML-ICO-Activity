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


DROP TABLE AAA_Tmp4_Acte_Caracteristique_Soins
DROP TABLE AAA_Tmp4_Acte_Caracteristique_SOS

SELECT        [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour,  max(A_Liste_UF_V3.[Poids de l'acte]) as Max_Acte_Soins
INTO AAA_Tmp3_Max_Dim_soins
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE A_Liste_UF_V3.[Dimension Parcours] like 'Soins'
group by Ho_Num_Num_sejour
Order by Ho_Num_Num_sejour


SELECT        [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour,  max(A_Liste_UF_V3.[Poids de l'acte]) as Max_Acte_SOS
INTO AAA_Tmp3_Max_Dim_SOS
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE A_Liste_UF_V3.[Dimension Parcours] like 'Soins Sup%'
group by Ho_Num_Num_sejour
Order by Ho_Num_Num_sejour


SELECT        min(cast([A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] as float)) as Id_Acte_Caracteristique, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour
INTO AAA_Tmp4_Acte_Caracteristique_Soins
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         AAA_Tmp3_Max_Dim_soins ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp3_Max_Dim_soins.Ho_Num_Num_sejour INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE A_Liste_UF_V3.[Dimension Parcours] like 'Soins' and A_Liste_UF_V3.[Poids de l'acte]=AAA_Tmp3_Max_Dim_soins.Max_Acte_Soins
GROUP BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour
ORDER BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour

SELECT        min(cast([A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] as float)) as Id_Acte_Caracteristique, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour
INTO AAA_Tmp4_Acte_Caracteristique_SOS
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         AAA_Tmp3_Max_Dim_SOS ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp3_Max_Dim_SOS.Ho_Num_Num_sejour INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE A_Liste_UF_V3.[Dimension Parcours] like 'Soins Sup%' and A_Liste_UF_V3.[Poids de l'acte]=AAA_Tmp3_Max_Dim_SOS.Max_Acte_SOS
GROUP BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour
ORDER BY [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour

DROP TABLE AAA_Tmp3_Max_Dim_soins
DROP TABLE AAA_Tmp3_Max_Dim_SOS
