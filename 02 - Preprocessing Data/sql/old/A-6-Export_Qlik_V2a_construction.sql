
DECLARE @dateVar_deb datetime = '20150101';
DECLARE @dateVar_fin datetime = '20230201';


DROP TABLE [dbo].[AAA_Export_Qlik1]


/* Liste de l'ensemble des actes avec les éléments caractéristiques */
SELECT        [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM], [A_Actes-ICO-2018-2021_Global0].NIP_original, [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour, [A_Actes-ICO-2018-2021_Global0].Date_début_acte, 
                         [A_Actes-ICO-2018-2021_Global0].Date_fin_acte, AAA_Tmp10_Table_Acte_Caract_YN.Acte_Caracteristique_Soins, AAA_Tmp10_Table_Acte_Caract_YN.Acte_Caracteristique_Soins_Support, 
                         [A_Actes-ICO-2018-2021_Global0].Statut, A_Liste_UF_V3.Service, A_Liste_UF_V3.Activite, A_Liste_UF_V3.[Phase Parcours], A_Liste_UF_V3.[Dimension Parcours], A_Liste_UF_V3.[Poids de l'acte], AAA_Tmp2_J0V12.J0_V1, 
                         AAA_Tmp2_J0V12.J0_V2, AAA_Tmp3_Ddetfinsejour.Ddebsej, AAA_Tmp3_Ddetfinsejour.Dfinsej, AAA_Tmp7_Sejour_caracterises.Séquence_Parcours, AAA_Tmp7_Sejour_caracterises.Date_dernier_tt, 
                         AAA_Tmp7_Sejour_caracterises.J0_V3, AAA_Tmp7_Sejour_caracterises.J0_V4, AAA_Tmp7_Sejour_caracterises.Date_Validite_FA
INTO              AAA_Export_Qlik1
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         AAA_Tmp10_Table_Acte_Caract_YN ON [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] = AAA_Tmp10_Table_Acte_Caract_YN.[idImport-Actes-CCAM] INNER JOIN
                         A_Liste_UF_V3 ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Liste_UF_V3.UFX_Code INNER JOIN
                         AAA_Tmp2_J0V12 ON [A_Actes-ICO-2018-2021_Global0].NIP_original = AAA_Tmp2_J0V12.NIP_original INNER JOIN
                         AAA_Tmp3_Ddetfinsejour ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp3_Ddetfinsejour.Ho_Num_Num_sejour INNER JOIN
                         AAA_Tmp7_Sejour_caracterises ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = AAA_Tmp7_Sejour_caracterises.Ho_Num_Num_sejour