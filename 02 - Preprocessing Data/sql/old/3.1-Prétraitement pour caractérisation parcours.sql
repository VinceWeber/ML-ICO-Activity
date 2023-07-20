/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        A_Caracterisation_Parcours.NIP_original, A_Caracterisation_Parcours.Ho_Num_Num_sejour, A_Caracterisation_Parcours.Ddebsej, A_Caracterisation_Parcours.J0_V1, A_Caracterisation_Parcours.J0_V2, 
                         A_Caracterisation_Parcours.Annee_NIP, A_Caracterisation_Parcours.poids_Séjour_soins, A_Caracterisation_Parcours.Site_idSite, A_Caracterisation_Parcours.Séquence_Parcours, A_Caracterisation_Parcours.Date_dernier_tt, 
                         A_Caracterisation_Parcours.J0_V3, A_Caracterisation_Parcours.J0_V4, A_Qualif_sejour2.Poids_Dim_Soins, A_Qualif_sejour2.Poids_Dim_SOS, A_Qualif_sejour2.Dfinsej, A_Qualif_sejour2.Dfinsej - CONVERT(DATETIME, A_Caracterisation_Parcours.Ddebsej, 102) AS duree_sejour
INTO A_Qualif_Sejour3
FROM            A_Caracterisation_Parcours INNER JOIN
                         A_Qualif_sejour2 ON A_Caracterisation_Parcours.Ho_Num_Num_sejour = A_Qualif_sejour2.Ho_Num_Num_sejour
