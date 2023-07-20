
/*
	EN COURS DE TRAVAIL 
	Données d'entrée = 
		A_Actes-ICO-2018-2021_Global0
		A_Liste_UF_V3
		AAA_Tmp7_Sejour_caracterises

	Processus = 
		Défini un médecin référent du parcours ou patient

	Données de sortie = 
		
		
*/


SELECT        TOP (100) [A_Actes-ICO-2018-2021_Global0].NIP_original, A_Liste_UF_V3.[Phase Parcours], A_Liste_UF_V3.[Poids de l'acte], [A_Actes-ICO-2018-2021_Global0].Ressource_Med_INX_INX_Code, 
                         [A_Actes-ICO-2018-2021_Global0].INX_Code_Lib, [A_Actes-ICO-2018-2021_Global0].INX_Code_Spe, A_Liste_UF_V3.[Dimension Parcours], AAA_Tmp7_Sejour_caracterises.Séquence_Parcours, A_Liste_UF_V3.Activite, 
                         [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM]
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         AAA_Tmp7_Sejour_caracterises ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp7_Sejour_caracterises.Ho_Num_Num_sejour INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code
WHERE        (AAA_Tmp7_Sejour_caracterises.Séquence_Parcours LIKE 'SUIVI_LT') AND (A_Liste_UF_V3.Activite LIKE 'Consultations') AND (A_Liste_UF_V3.[Dimension Parcours] LIKE 'Soins') 
				and (INX_Code_Spe like 'RADIOTHERAPIE' or INX_Code_Spe like 'ONCO%' or INX_Code_Spe like 'CHIR%')
				and NIP_original like 'A2018%'
ORDER BY [A_Actes-ICO-2018-2021_Global0].NIP_original, cast([A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] as float)