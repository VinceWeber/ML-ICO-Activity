/*
	Données d'entrée = 
		A_Actes-ICO-2018-2021_Global0

	Processus = 
		Défini l'équipement mobilisé pour l'acte considéré
		Propose un lien entre ID acte et le code équipement mobilisé (par UF)

	Données de sortie = 
		AAA_Tmp11_Actes_Equipements
		
*/



DROP TABLE AAA_Tmp11_Actes_Equipements

/* Liste des equipements issu des MVT */
SELECT [idImport-Actes-CCAM]
      ,[Equipements_Code_Equipement]
      ,[Source]
INTO AAA_Tmp11_Actes_Equipements
  FROM [ICOActivite].[dbo].[A_Actes-ICO-2018-2021_Global0]
  WHERE Source like 'MVT'

UNION 

/* Liste des equipements issu des CCAM */
SELECT        [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM], A_Liste_Equipement_2022.Code_Equipement, [A_Actes-ICO-2018-2021_Global0].Source
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Liste_UF_Equip_2022 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_Equip_2022.UFX_UFX_Code INNER JOIN
                         A_Liste_Equipement_2022 ON A_Liste_UF_Equip_2022.Equipements_idEquipement = A_Liste_Equipement_2022.idEquipement
WHERE        ([A_Actes-ICO-2018-2021_Global0].Source LIKE 'CCAM')


UNION 

/* Liste des equipements issu des NGAP */
SELECT         [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM],A_Liste_Equipement_2022.Code_Equipement, [A_Actes-ICO-2018-2021_Global0].Source
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Liste_UF_Equip_2022 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_Equip_2022.UFX_UFX_Code INNER JOIN
                         A_Liste_Equipement_2022 ON A_Liste_UF_Equip_2022.Equipements_idEquipement = A_Liste_Equipement_2022.idEquipement
WHERE        ([A_Actes-ICO-2018-2021_Global0].Source LIKE 'NGAP')

