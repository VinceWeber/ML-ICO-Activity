/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        A_Caracterisation_Parcours.NIP_original, MAX(DISTINCT [A_Actes-ICO-2018-2021_Global0].INX_Code_Lib) AS INX_Code_lib_ref, MAX(DISTINCT [A_Actes-ICO-2018-2021_Global0].INX_Code_Spe) AS INX_Code_Spe_ref, 
                         MAX(DISTINCT [A_Actes-ICO-2018-2021_Global0].Ressource_Med_INX_INX_Code) AS RessourceMedcode_ref
INTO A_Med_ref
FROM            A_Caracterisation_Parcours INNER JOIN
                         [A_Actes-ICO-2018-2021_Global0] ON A_Caracterisation_Parcours.Ho_Num_Num_sejour = [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour INNER JOIN
                         A_Listing_UF ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Listing_UF.UFX_Code
WHERE        (A_Caracterisation_Parcours.Séquence_Parcours LIKE 'SUIVI_LT') AND (A_Listing_UF.[Poids de l'acte] = 10) AND (A_Listing_UF.[Dimension Parcours] = 'Soins') AND 
                         ([A_Actes-ICO-2018-2021_Global0].INX_Code_Spe = 'RADIOTHERAPIE' OR
                         [A_Actes-ICO-2018-2021_Global0].INX_Code_Spe = 'ONCOLOGIE MEDICALE' OR
                         [A_Actes-ICO-2018-2021_Global0].INX_Code_Spe = 'CHIRURGIE')
GROUP BY A_Caracterisation_Parcours.NIP_original
ORDER BY A_Caracterisation_Parcours.NIP_original