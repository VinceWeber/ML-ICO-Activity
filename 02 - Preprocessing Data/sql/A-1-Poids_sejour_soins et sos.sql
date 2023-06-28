/*
Pods séjour
	Données d'entrée = A_Actes-ICO-2018-2021_Global0

	Processus = Associe un "poids" de séjour à chaque n° de séjour
				Le poids des actes du séjour est défini dans la table Liste UF_V3

	Données de sortie = AAA_Tmp_Poids_Sejours2

*/
drop table AAA_Tmp_Poids_Sejours2

SELECT   [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, sum(A_Liste_UF_V3.[Poids de l'acte]) as Poids_Sejour_DS, sum(0) as Poids_Sejour_DSOS, A_Liste_UF_V3.[Dimension Parcours] 
INTO AAA_Tmp_Poids_Sejours
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE A_Liste_UF_V3.[Dimension Parcours]='Soins'
GROUP BY Ho_Num_Num_sejour,A_Liste_UF_V3.[Dimension Parcours]

UNION

SELECT   [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, sum(0)  as Poids_Sejour_DS ,sum(A_Liste_UF_V3.[Poids de l'acte]) as Poids_Sejour_DSOS, A_Liste_UF_V3.[Dimension Parcours]
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE A_Liste_UF_V3.[Dimension Parcours]='Soins Support'
GROUP BY Ho_Num_Num_sejour,A_Liste_UF_V3.[Dimension Parcours]

UNION

SELECT   [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, sum(0)  as Poids_Sejour_DS ,sum(0) as Poids_Sejour_DSOS, A_Liste_UF_V3.[Dimension Parcours]
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE A_Liste_UF_V3.[Dimension Parcours]='NC' or A_Liste_UF_V3.[Dimension Parcours]='Hors_ICO'
GROUP BY Ho_Num_Num_sejour,A_Liste_UF_V3.[Dimension Parcours]

ORDER BY Ho_Num_Num_sejour


/* STEP 2  CONCATENATION POUR FINALISER LA TABLE N° SEJOUR + POIDS SEJOURS */

SELECT [Ho_Num_Num_sejour]
      ,max([Poids_Sejour_DS]) as Poids_Sejour_DS 
      ,max([Poids_Sejour_DSOS]) as Poids_Sejour_DSOS
  INTO AAA_Tmp_Poids_Sejours2
  FROM AAA_Tmp_Poids_Sejours
  GROUP BY [Ho_Num_Num_sejour]
  order by [Ho_Num_Num_sejour]

  drop table AAA_Tmp_Poids_Sejours