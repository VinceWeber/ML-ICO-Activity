/****** Script for SelectTopNRows command from SSMS  ******/
SELECT        [A_Actes-ICO-2018-2021_Global0].NIP_original, A_Med_ref.INX_Code_lib_ref AS INX_Code_Lib_refx, A_Med_ref.INX_Code_Spe_ref AS INX_Code_Spe_refx, A_Med_ref.RessourceMedcode_ref AS RessourceMedcode_refx
INTO A_Med_ref_AllNips
FROM            [A_Actes-ICO-2018-2021_Global0] INNER JOIN
                         A_Med_ref ON [A_Actes-ICO-2018-2021_Global0].NIP_original = A_Med_ref.NIP_original
GROUP BY [A_Actes-ICO-2018-2021_Global0].NIP_original, A_Med_ref.INX_Code_lib_ref, A_Med_ref.INX_Code_Spe_ref, A_Med_ref.RessourceMedcode_ref

UNION

SELECT        [A_Actes-ICO-2018-2021_Global0].NIP_original, '' AS INX_Code_Lib_refx, '' AS INX_Code_Spe_refx, '' AS RessourceMedcode_refx
FROM            [A_Actes-ICO-2018-2021_Global0] 
WHERE [A_Actes-ICO-2018-2021_Global0].NIP_original NOT IN ( SELECT [NIP_original]   FROM [ICOActivite].[dbo].[A_Med_ref])

GROUP BY [A_Actes-ICO-2018-2021_Global0].NIP_original
ORDER BY [A_Actes-ICO-2018-2021_Global0].NIP_original