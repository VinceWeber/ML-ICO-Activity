USE [ICO_Activite]
GO
/****** Object:  UserDefinedFunction [dbo].[F_Filter_Aggreg]    Script Date: 01/11/2023 17:37:30 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date, ,>
-- Description:	<Description, ,>
-- =============================================
ALTER FUNCTION [dbo].[F_Filter_Aggreg]
(	-- Add the parameters for the function here
	@Type nvarchar(100),
	@Filter_Value nvarchar(500), 
	@Type_Result nvarchar(500)
)
RETURNS nvarchar(500)

AS
BEGIN
	-- Declare the return variable here
	DECLARE 
		@ResultVar nvarchar(500),
		@ErrorMEss nvarchar(500),
		@filtre1_query_FROM nvarchar(500),
		@filtre1_query_WHERE nvarchar(500)



	SET @ErrorMEss='ERROR ON Filter Parameter in F_Filter_Aggreg Function .' + 
							@Type + ' was given,but an integer from 0 to 7 is expected'


	IF @Type='1'
		BEGIN 
			SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Service
			SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Service=''' + @Filter_Value + ''' '
		END
	ELSE
		IF @Type='2'
			BEGIN 
				SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Activite
				SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Activite=''' + @Filter_Value+ ''' '
			END
		ELSE
			IF @Type='3'
				BEGIN 
					SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Phase_Parcours
					SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Phase_Parcours=''' + @Filter_Value+ ''' '
				END
			ELSE
				IF @Type='4'
					BEGIN 
						SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX '  + ' ' -- ADD A FILTER BY UFX.Dimension_Parcours
						SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Dimension_Parcours=''' + @Filter_Value+ ''' '
					END
				ELSE
					IF @Type='5'
						BEGIN 
							SET @filtre1_query_FROM= ',[ICO_Activite].[dbo].[Listing_UF_V3] as T_UFX ' + ' ' -- ADD A FILTER BY UFX.Poids_Acte
							SET @filtre1_query_WHERE= 'AND Table_Actes.UFX=T_UFX.UFX_Code AND T_UFX.Poids_acte=''' + @Filter_Value+ ''' '
						END
					ELSE
						IF @Type='6'
							BEGIN 
								SET @filtre1_query_FROM= '' -- ADD A FILTER BY UFX.Type_Sequence
								SET @filtre1_query_WHERE= 'AND Table_Type_Seq.Type_Sequence=''' + @Filter_Value+ ''' '
							END
						ELSE
							IF @Type='7'
								BEGIN 
									SET @filtre1_query_FROM= 'Not Provided yet'  -- ADD A FILTER BY Type_parcours
									SET @filtre1_query_WHERE= 'Not Provided yet' + @Filter_Value+ ''' '
								END
							ELSE
								IF @Type='8'
									BEGIN
										SET @filtre1_query_FROM= ''
										SET @filtre1_query_WHERE='AND Table_Actes.R_NGAP='''  + @Filter_Value+ ''' ' --R_NGAP
									END
								ELSE								
								IF @Type='9'
									BEGIN
										SET @filtre1_query_FROM= ''
										SET @filtre1_query_WHERE='AND Table_Actes.R_CCAM='''+ @Filter_Value+ ''' ' --R_CCAM
									END
								ELSE								
								IF @Type='10'
									BEGIN
										SET @filtre1_query_FROM= ''
										SET @filtre1_query_WHERE='AND Table_Actes.Statut='''+ @Filter_Value+ ''' '  --Statut
									END
								ELSE								
								IF @Type='11'
									BEGIN
										SET @filtre1_query_FROM= ''
										SET @filtre1_query_WHERE='AND Table_Actes.UFH='''+ @Filter_Value+ ''' '  --UFH
									END
								ELSE								
								IF @Type='12'
									BEGIN
										SET @filtre1_query_FROM= ''
										SET @filtre1_query_WHERE='AND Table_Actes.INX='''+ @Filter_Value+ ''' '  --INX
									END
								ELSE								
								IF @Type='0'
									BEGIN
										SET @filtre1_query_FROM= ''
										SET @filtre1_query_WHERE=''
									END
								ELSE
									return cast(@ErrorMEss as int);  --RAISE AN ERROR !

	SET @ErrorMEss='ERROR ON Filter Parameter in F_Filter_Aggreg Function .' + 
							@Type_Result + ' was given,but FROM or WHERE is expected'

	IF @Type_Result='FROM'
		SET @RESULTVar='
									' + @filtre1_query_FROM + '
		'
	ELSE
		IF @Type_Result='WHERE'
			SET @RESULTVar='
									' + @filtre1_query_WHERE + '
		'
		ELSE
			return cast(@ErrorMEss as int);

	-- Return the result of the function
	RETURN @ResultVar

END
