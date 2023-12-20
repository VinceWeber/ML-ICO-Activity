

#Definition d'une fonction de création de la requete d'aggregation
def req_aggreg (Aggreg_parameters):
    
    Requete="""EXECUTE [dbo].[ReportCarePathActivtiy_By_actes] """ \
    + Aggreg_parameters['Report_type'] + "," \
		+ "'" + Aggreg_parameters['Aggreg_type'] + "'"+ "," \
			+ Aggreg_parameters['Date_ref'] + "," \
				+ Aggreg_parameters['Start_at_item'] + "," \
					+ Aggreg_parameters['Stop_at_item'] + "," \
						+ "'" + Aggreg_parameters['Method'] + "'" + "," \
							+ Aggreg_parameters['Type_filter1'] + "," \
								+ "'" + Aggreg_parameters['Val_filter1'] + "'" + "," \
									+ Aggreg_parameters['Type_filter2'] + "," \
										+ "'" + Aggreg_parameters['Val_filter2'] + "'" + "," \
											+ "'" + Aggreg_parameters['Param_J0'] + "'"

    return Requete


def Create_dataset (Create_dataset_parameters):
    from datetime import datetime
    from datetime import timedelta
    import pandas as pd
    import Parcours_Classes as PC

    My_NIP_filter_1rst_date=datetime.strptime(Create_dataset_parameters['My_NIP_filter_1rst_date'], '%m-%d-%Y %H:%M:%S')
    My_NIP_filter_2nd_date=My_NIP_filter_1rst_date + timedelta(days=Create_dataset_parameters['My_NIP_filter_2nd_date_delta_in_days'])

    Site=Create_dataset_parameters['Site']

    Mydataset_date1=datetime.strptime(Create_dataset_parameters['Start_Window_time'], '%m-%d-%Y %H:%M:%S')
    Mydataset_date2=datetime.strptime(Create_dataset_parameters['End_Window_time'], '%m-%d-%Y %H:%M:%S')

    Caract_Df_SH = pd.DataFrame.from_dict(PC.Caracteristiques_Dataset_Parcours(1, My_NIP_filter_1rst_date,My_NIP_filter_2nd_date,Site,Mydataset_date1,Mydataset_date2).get_x())

    print(Caract_Df_SH)
    return Caract_Df_SH


def plot_carepath(dataset):
    import matplotlib.pyplot as plt
    import seaborn as sns
    # Fonction pour générer des valeurs uniques
    def generate_unique_values(df):
        unique_values = {}
        new_column = []
        for nip in df['NIP']:
            if nip not in unique_values:
                unique_values[nip] = len(unique_values) + 1
            new_column.append(unique_values[nip])
        return new_column

    # Ajoutez une nouvelle colonne avec des valeurs uniques
    Temp_dataset=dataset.copy()
    Temp_dataset['ID_NIP'] = generate_unique_values(dataset)

    #Affichage des parcours
    fig, axs = plt.subplots(1, 1, figsize=(15, 6))

    axs.set_title('Carepathes')
    Myscatterplot=sns.scatterplot(data=Temp_dataset, x='ID_NIP', y='J_Parcours_V1',markers='Activite', hue='Service')

    del Temp_dataset
    return Myscatterplot

def chk_Agg_param(Agg_param):
    if Agg_param!=None:
        result= True
    else:
        result= False
    return result

def get_Aggreg_Dataset(Agg_param1,Agg_param2=None,Agg_param3=None,Agg_param4=None):
    import pandas as pd
    import Sql_Alchemy_Classes as AlSQL

    
    agg_list=[]
    if chk_Agg_param(Agg_param1):
        agg_list.append(Agg_param1)
    if chk_Agg_param(Agg_param2):
        agg_list.append(Agg_param2)
    if chk_Agg_param(Agg_param3):
        agg_list.append(Agg_param3)
    if chk_Agg_param(Agg_param4):
        agg_list.append(Agg_param4)       

    print("Nb of aggregations dimensions :" + str(len(agg_list)))

    df_out=pd.DataFrame()

    for Agg_param in agg_list: 
        
        #definition de la requete
        Requete=req_aggreg (Agg_param)    
        
        #Get the dataset
        df=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,True)
        
        df.replace('','0',inplace=True)
        for col in df.columns[1:]:  # Starting from the second column onwards
            df[col] = pd.to_numeric(df[col], errors='coerce')

        #Add information of the parameters used
        df['FT1']=Agg_param['Type_filter1']
        df['FV1']=Agg_param['Val_filter1']
        df['FT2']=Agg_param['Type_filter2']
        df['FV2']=Agg_param['Val_filter2']


        #Merge this newdataset with the previous one
        df_out = pd.concat([df_out, df], axis=0)
    
        dict_out={
            'Nb_dim': str(len(agg_list)),
            'df': df_out
        }
    return dict_out



def GetDistanceMatrix(Parcours_dict, Aggreg_parameters):
    import numpy as np
    import dtw as dtw

    #Aggreg_Patients.replace('','0',inplace=True)

    #for col in Aggreg_Patients.columns[1:]:  # Starting from the second column onwards
    #    Aggreg_Patients[col] = pd.to_numeric(Aggreg_Patients[col], errors='coerce')

    #import matplotlib.pyplot as plt

    Timesteps=int(Aggreg_parameters['Stop_at_item'])-int(Aggreg_parameters['Start_at_item'])
    Parcours_dict['df'].sort_values(['NIP', 'FV1','FV2'], ascending=[True, True, True], inplace=True)

    data_to_plot = Parcours_dict['df'].iloc[:,1:(Timesteps+1)] 
    #data_to_plot = data_to_plot.tail(500)

    Nb_NIP=len(Parcours_dict['df']['NIP'].unique())
    Nb_dim=int(Parcours_dict['Nb_dim'])
    result = np.zeros((Nb_NIP, Nb_NIP))

    import time
    start_time = time.time()  # Temps de départ

    for i in range(Nb_NIP-1):
        for j in range(i,Nb_NIP-1):   #Compute only half of the matrix as the distance is symetric !
            
            line_i=i*Nb_dim
            line_j=j*Nb_dim

            query=data_to_plot.iloc[line_i : line_i + Nb_dim ,:].values # For one dimension Time serie
            template=data_to_plot.iloc[line_j : line_j + Nb_dim ,:].values # For one dimension Time serie
            #print("i= " +str(i) +  " j= " + str(j))
            #print("query" + str(query))
            #print("query" + str(template))

            result[i, j] = dtw.dtw(query, template, distance_only=True).distance
            result[j, i] = result[i, j]

        if i % 100 == 0:  # Mise à jour toutes les 10 itérations
            current_time = time.time() - start_time  # Temps écoulé jusqu'à présent
            time_per_iteration = current_time / (i + 1)  # Temps par itération
            remaining_time = time_per_iteration * (Nb_NIP-1 - i - 1)/60  # Temps restant
            print(f"Iteration {i}/{Nb_NIP-1} - Temps restant estimé: {remaining_time:.2f} minutes")

    end_time = time.time() - start_time  # Temps total écoulé
    print(f"Durée totale de traitement: {end_time:.2f} secondes")

    return result

def GetDistanceMatrix_Parrallel(Parcours_dict, Aggreg_parameters,nthreads):
    import numpy as np
    import threading
    import time

    #Aggreg_Patients.replace('','0',inplace=True)

    #for col in Aggreg_Patients.columns[1:]:  # Starting from the second column onwards
    #    Aggreg_Patients[col] = pd.to_numeric(Aggreg_Patients[col], errors='coerce')

    #import matplotlib.pyplot as plt

    Timesteps=int(Aggreg_parameters['Stop_at_item'])-int(Aggreg_parameters['Start_at_item'])
    Parcours_dict['df'].sort_values(['NIP', 'FV1','FV2'], ascending=[True, True, True], inplace=True)

    data= Parcours_dict['df'].iloc[:,1:(Timesteps+1)] 

    Nb_NIP=len(Parcours_dict['df']['NIP'].unique())
    Nb_dim=int(Parcours_dict['Nb_dim'])

    result={}

    start_time = time.time()  # Temps de départ

    List_Cn=Parrallelization_parameters_half_sq_matrix(nthreads, Nb_NIP)
    print("List_Cn " + str(List_Cn))
    
    
    for i in range(len(List_Cn)-1):
        result[f'part_{i+1}/{nthreads}'] = Distance_matrix_compute(List_Cn[i], List_Cn[i + 1], data, Nb_dim, Nb_NIP)


    end_time = time.time() - start_time  # Temps total écoulé
    print(f"Durée totale de traitement: {end_time:.2f} secondes")

    #Group the different results.
        #number of results = nthreads
    PDM=[]
    PDM_T=[]
    for i in range(len(List_Cn)-1):
        if i==0:
            PDM=result[f'part_{i+1}/{nthreads}']['PDM']
            PDM_T=result[f'part_{i+1}/{nthreads}']['PDM_T']
        else:
            PDM=np.concatenate((PDM , result[f'part_{i+1}/{nthreads}']['PDM']), axis=1)
            PDM_T=np.concatenate((PDM_T , result[f'part_{i+1}/{nthreads}']['PDM_T']), axis=0)        

    PDM_Full=PDM +PDM_T

    return PDM_Full


def Distance_matrix_compute(Start_col,End_col,data,Nb_dim,Nb_lines):
    import numpy as np
    import dtw as dtw

    print("Start Distance_matrix_compute : Startcol= " +str(Start_col) +  "Endcol=  " +str(End_col) + " dim : " + str(Nb_dim) + " Nb_lines : " + str(Nb_lines))
    print("data = " + str(data))
    
    Nb_col=End_col-Start_col
    Partial_dist_matrix = np.zeros((Nb_lines,Nb_col))
    Partial_dist_matrix_T = np.zeros((Nb_col,Nb_lines))
    print("Parial dist_matrix dim : " + str(Nb_lines) +"(Nblines) " + str(Nb_col) + "Nbcol")
    for i in range(0,Nb_lines):
        for j in range(Start_col,End_col):   #Compute only half of the matrix as the distance is symetric !
            if j>=i:    
                
                line_i=(i)*Nb_dim
                line_j=(j)*Nb_dim

                query=data.iloc[line_i : line_i + Nb_dim ,:].values 
                template=data.iloc[line_j : line_j + Nb_dim ,:].values 
                print("i= " +str(i) +  " j= " + str(j))
                print("query" + str(query))
                print("query" + str(template))

                Partial_dist_matrix[i, j-Start_col] = dtw.dtw(query, template, distance_only=True).distance
                Partial_dist_matrix_T[j-Start_col,i] = Partial_dist_matrix[i, j-Start_col]
                print("dist" + str(Partial_dist_matrix[i, j-Start_col]))

        dic_out={
                'PDM':Partial_dist_matrix,
                'PDM_T':Partial_dist_matrix_T
                }
    print("PDM" + str(Partial_dist_matrix))
    print("PDM_T" + str(Partial_dist_matrix_T))

    return dic_out





def Parrallelization_parameters_half_sq_matrix(nthreads, n_col):
    import numpy as np
    import math 
    list_Cn=np.zeros((nthreads+1))

    S0=n_col**2/(2*nthreads)
    

    for i in range(1,nthreads):
        if i==0:
            list_Cn[0]=math.sqrt(2*S0)
        else:
            list_Cn[i]=math.sqrt(2*S0+list_Cn[i-1]**2)
    
    list_Cn[nthreads]=n_col
    IntList_Cn=[int(x) for x in list_Cn]

    return IntList_Cn 

