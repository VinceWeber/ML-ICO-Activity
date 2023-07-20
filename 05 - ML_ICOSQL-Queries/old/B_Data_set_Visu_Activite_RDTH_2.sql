/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        A_DAtaSet_Sejours.Site, A_DAtaSet_Sejours.NIP, A_DAtaSet_Sejours.Poids_Dim_SOS, A_DAtaSet_Sejours.Séquence_Parcours, A_Med_ref_AllNips.INX_Code_Lib_refx, A_DAtaSet_Sejours.asPoids_Dim_Soins, 
                         A_DAtaSet_Sejours.J_Parcours_V1, A_DAtaSet_Sejours.J_Parcours_V3
FROM            A_DAtaSet_Sejours INNER JOIN
                         A_Med_ref_AllNips ON A_DAtaSet_Sejours.NIP = A_Med_ref_AllNips.NIP_original
WHERE        (A_DAtaSet_Sejours.NIP IN
                             (SELECT        NIP
                               FROM            B_Rdth))