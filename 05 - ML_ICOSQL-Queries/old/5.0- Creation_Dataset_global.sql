/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        [A_Actes-ICO-2018-2021_Global0].[idImport-Actes-CCAM] AS id_acte, [A_Actes-ICO-2018-2021_Global0].Site_idSite AS Site, [A_Actes-ICO-2018-2021_Global0].NIP_original AS NIP, 
                         [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour AS Num_Sejour, A_Qualif_Sejour3.Ddebsej, A_Qualif_Sejour3.Dfinsej, A_Qualif_Sejour3.J0_V2, A_Qualif_Sejour3.J0_V1, A_Qualif_Sejour3.J0_V3, A_Qualif_Sejour3.J0_V4,
                          DATEDIFF(day,A_Qualif_Sejour3.J0_V1,A_Qualif_Sejour3.Ddebsej) AS J_Parcours_V1, DATEDIFF(day,A_Qualif_Sejour3.J0_V2,A_Qualif_Sejour3.Ddebsej) AS J_Parcours_V2, DATEDIFF(day,A_Qualif_Sejour3.J0_V3,A_Qualif_Sejour3.Ddebsej) AS J_Parcours_V3, DATEDIFF(day,A_Qualif_Sejour3.J0_V4,A_Qualif_Sejour3.Ddebsej) AS J_Parcours_V4, A_Qualif_Sejour3.Poids_Dim_Soins, A_Qualif_Sejour3.Poids_Dim_SOS, A_Qualif_Sejour3.Séquence_Parcours, 
                         A_Med_ref_AllNips.RessourceMedcode_refx, A_Med_ref_AllNips.INX_Code_Lib_refx, A_Med_ref_AllNips.INX_Code_Spe_refx, A_Listing_UF.UFX_Code, A_Listing_UF.UFX_Code_Lib, A_Listing_UF.Service, A_Listing_UF.Activite, 
                         A_Listing_UF.[Info complémentaire], A_Listing_UF.[Phase Parcours], A_Listing_UF.[Dimension Parcours], A_Listing_UF.[Poids de l'acte]
Into A_DataSet_Global
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Qualif_Sejour3 ON [A_Actes-ICO-2018-2021_Global0].Ho_Num_Num_sejour = A_Qualif_Sejour3.Ho_Num_Num_sejour INNER JOIN
                         A_Med_ref_AllNips ON [A_Actes-ICO-2018-2021_Global0].NIP_original = A_Med_ref_AllNips.NIP_original INNER JOIN
                         A_Listing_UF ON [A_Actes-ICO-2018-2021_Global0].UFX_UFX_Code = A_Listing_UF.UFX_Code
ORDER BY NIP,Num_Sejour,id_acte