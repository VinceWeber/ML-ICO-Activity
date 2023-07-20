
/*
	Données d'entrée = 
		A_Actes-ICO-2018-2021_Global0
		A_Liste_UF_V3
		AAA_Tmp7_Sejour_caracterises

	Processus = 
		Défini si l'ID d'un acte de la table global est considéré comme un acte acractéristique du parcours (Yes/No)

	Données de sortie = 
		AAA_Tmp10_Table_Acte_Caract_YN
		
*/



DROP TABLE AAA_Tmp10_Table_Acte_Caract_YN

/* Liste Acte CCAM avec Acte caracteristique Soins */

SELECT        [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM], [A_Actes-ICO-2018-2021_Global0].NIP_original, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour,
				AAA_Tmp4_Acte_Caracteristique_Soins.Id_Acte_Caracteristique,  'YES' as Acte_Caracteristique_Soins,  NULL as Acte_Caracteristique_Soins_Support
INTO AAA_Tmp10_Liste_acte_caract_dim_soins
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         AAA_Tmp4_Acte_Caracteristique_Soins ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp4_Acte_Caracteristique_Soins.Ho_Num_Num_sejour


/* Liste Acte CCAM avec Acte caracteristique Soins Support*/
SELECT        [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM], [A_Actes-ICO-2018-2021_Global0].NIP_original, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour,  
                         AAA_Tmp4_Acte_Caracteristique_SOS.Id_Acte_Caracteristique,  NULL as Acte_Caracteristique_Soins,  'YES' as Acte_Caracteristique_Soins_Support
INTO AAA_Tmp10_Liste_acte_caract_dim_SOS
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         AAA_Tmp4_Acte_Caracteristique_SOS ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp4_Acte_Caracteristique_SOS.Ho_Num_Num_sejour

/*
SELECT        [idImport-Actes-CCAM], NIP_original, Ho_Num_Num_sejour, NULL AS Acte_Caracteristique_Soins, NULL AS Acte_Caracteristique_Soins_Support
FROM            [A_Actes-ICO-2018-2021_Global0]

WHERE [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] NOT IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_soins])  and 
      [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] NOT IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_SOS])
*/

/* ajouter NULL aux deux dimensions a tous les items qui n'apparaissent dans aucune des deux tables ci-dessus */

SELECT        [idImport-Actes-CCAM], NIP_original, Ho_Num_Num_sejour, NULL AS Acte_Caracteristique_Soins, NULL AS Acte_Caracteristique_Soins_Support
INTO AAA_Tmp10_Table_Acte_Caract_YN
FROM            [A_Actes-ICO-2018-2021_Global0]

WHERE [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] NOT IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_soins])  and 
      [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] NOT IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_SOS])

UNION

SELECT [idImport-Actes-CCAM],[NIP_original],[Ho_Num_Num_sejour], 'YES' AS Acte_Caracteristique_Soins, NULL AS Acte_Caracteristique_Soins_Support
FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_soins]
WHERE [idImport-Actes-CCAM] NOT IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_SOS])

UNION

SELECT [idImport-Actes-CCAM],[NIP_original],[Ho_Num_Num_sejour], NULL AS Acte_Caracteristique_Soins, 'YES' AS Acte_Caracteristique_Soins_Support
FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_SOS]
WHERE [idImport-Actes-CCAM] NOT IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_soins])

UNION

SELECT [idImport-Actes-CCAM],[NIP_original],[Ho_Num_Num_sejour], NULL AS Acte_Caracteristique_Soins, 'YES' AS Acte_Caracteristique_Soins_Support
FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_SOS]
WHERE [idImport-Actes-CCAM] IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_soins]) and 
	  [idImport-Actes-CCAM] IN ( SELECT [idImport-Actes-CCAM]  FROM [ICOActivite].[dbo].[AAA_Tmp10_Liste_acte_caract_dim_SOS])

/* Effacer les tables intermediaires */

DROP TABLE AAA_Tmp10_Liste_acte_caract_dim_soins
DROP TABLE AAA_Tmp10_Liste_acte_caract_dim_SOS
