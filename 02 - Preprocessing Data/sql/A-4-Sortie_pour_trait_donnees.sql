/*
Sortie intermédiaire pour traitement des données
	Données d'entrée = 
		A_Actes-ICO-2018-2021_Global0
		AAA_Tmp3_Ddetfinsejour
		AAA_Tmp2_J0V12
		AAA_Tmp_Poids_Sejours2

	Processus = 
		Formate une sortie d'un jeu de donnée pour lancer une routine de définition des séquences de parcours "INIT, TRAIT, SUVI_CT, SUIVI_LT"

	Données de sortie = 
		AAA_Tmp5_Export_Pour_Trait_Donnée
*/



DROP TABLE AAA_Tmp5_Export_Pour_Trait_Donnée
SELECT        [A_Actes-ICO-2018-2021_Global0].NIP_original, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, AAA_Tmp3_Ddetfinsejour.Ddebsej, AAA_Tmp2_J0V12.J0_V1, AAA_Tmp2_J0V12.J0_V2, 
                          SUBSTRING ([A_Actes-ICO-2018-2021_Global0].NIP_original,2,4) as Annee_NIP,AAA_Tmp_Poids_Sejours2.Poids_Sejour_DS, AAA_Tmp_Poids_Sejours2.Poids_Sejour_DSOS
INTO AAA_Tmp5_Export_Pour_Trait_Donnée
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         AAA_Tmp3_Ddetfinsejour ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp3_Ddetfinsejour.Ho_Num_Num_sejour INNER JOIN
                         AAA_Tmp2_J0V12 ON [A_Actes-ICO-2018-2021_Global0].NIP_original = AAA_Tmp2_J0V12.NIP_original INNER JOIN
                         AAA_Tmp_Poids_Sejours2 ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp_Poids_Sejours2.Ho_Num_Num_sejour
GROUP BY [A_Actes-ICO-2018-2021_Global0].NIP_original, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, AAA_Tmp3_Ddetfinsejour.Ddebsej, AAA_Tmp2_J0V12.J0_V1, AAA_Tmp2_J0V12.J0_V2, 
                          AAA_Tmp_Poids_Sejours2.Poids_Sejour_DS, AAA_Tmp_Poids_Sejours2.Poids_Sejour_DSOS
ORDER BY NIP_original, Ho_Num_Num_sejour