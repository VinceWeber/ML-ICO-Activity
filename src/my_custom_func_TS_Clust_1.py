

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

def req_aggreg_V2 (Aggreg_parameters):
    
    Requete="""EXECUTE [dbo].[ReportCarePathActivtiy_By_actes_V2] """ \
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
											+ "'" + Aggreg_parameters['Param_J0'] + "'" + "," \
                                                + "'" + Aggreg_parameters['Base'] + "'"

    return Requete


def Create_dataset (Create_dataset_parameters):
    from datetime import datetime
    from datetime import timedelta
    import pandas as pd
    import Parcours_Classes as PC

    My_NIP_filter_1rst_date=datetime.strptime(Create_dataset_parameters['DS_My_NIP_filter_1rst_date'], '%m-%d-%Y %H:%M:%S')
    My_NIP_filter_2nd_date=My_NIP_filter_1rst_date + timedelta(days=Create_dataset_parameters['DS_My_NIP_filter_2nd_date_delta_in_days'])

    Site=Create_dataset_parameters['DS_Site']

    Mydataset_date1=datetime.strptime(Create_dataset_parameters['DS_Start_Window_time'], '%m-%d-%Y %H:%M:%S')
    Mydataset_date2=datetime.strptime(Create_dataset_parameters['DS_End_Window_time'], '%m-%d-%Y %H:%M:%S')

    Caract_Df_SH = pd.DataFrame.from_dict(PC.Caracteristiques_Dataset_Parcours(1, My_NIP_filter_1rst_date,My_NIP_filter_2nd_date,Site,Mydataset_date1,Mydataset_date2).get_x())

    print(Caract_Df_SH)
    return Caract_Df_SH


def plot_carepath(dataset,filename_path):
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

    plt.savefig(filename_path)
    return Myscatterplot

def chk_Agg_param(Agg_param):
    if Agg_param!=None:
        result= True
    else:
        result= False
    return result

def get_Aggreg_Dataset(Agg_param1,Agg_param2=None,Agg_param3=None,Agg_param4=None,Agg_param5=None):
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
    if chk_Agg_param(Agg_param5):
        agg_list.append(Agg_param5)            

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

def get_Aggreg_Dataset2(list_param):
    import pandas as pd
    import Sql_Alchemy_Classes as AlSQL

    agg_list = list_param
    print(f"Number of aggregation dimensions: {len(agg_list)}")
    
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

    Timesteps=int(Aggreg_parameters['Stop_at_item'])-int(Aggreg_parameters['Start_at_item'])
    Parcours_dict['df'].sort_values(['NIP', 'FV1','FV2'], ascending=[True, True, True], inplace=True)

    data_to_plot = Parcours_dict['df'].iloc[:,1:(Timesteps+1)] 

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

def GetDistanceMatrix_Parrallel(Parcours_dict, Aggreg_parameters,percent_CPU):
    import numpy as np
    import threading
    import time


    #get the number of possible threads
    num_all_threads = threading.active_count()
    nthreads=min(int(percent_CPU*num_all_threads) ,num_all_threads)
    if nthreads==0:
        nthreads=1
    print(f"Use {nthreads} threads from {num_all_threads} available")

    Timesteps=int(Aggreg_parameters['Stop_at_item'])-int(Aggreg_parameters['Start_at_item'])
    Parcours_dict['df'].sort_values(['NIP', 'FV1','FV2'], ascending=[True, True, True], inplace=True)

    data= Parcours_dict['df'].iloc[:,1:(Timesteps+1)] 

    Nb_NIP=len(Parcours_dict['df']['NIP'].unique())
    Nb_dim=int(Parcours_dict['Nb_dim'])

    List_Cn=Parrallelization_parameters_half_sq_matrix(nthreads, Nb_NIP)
    print("List_Cn " + str(List_Cn))

    #initialistion de la variable 
    Thread_output={}
    threads=[]

    for i in range(nthreads):    
        #lancement du thread
        thread = threading.Thread(target=Distance_matrix_compute,
                                  args=(List_Cn[i], List_Cn[i + 1], data, Nb_dim, Nb_NIP, Thread_output),
                                  name=f'Thread {i}')
        threads.append(thread)
        thread.start()

    for thread in (threads): 
        thread.join()

    #Group the different results.
    PDM=[]
    PDM_T=[]
     
    for thread in (threads): 
        if thread.name=='Thread 0':
            PDM=Thread_output[f'{thread.name}']['PDM']
            PDM_T=Thread_output[f'{thread.name}']['PDM_T']
        else:
            PDM=np.concatenate((PDM ,Thread_output[f'{thread.name}']['PDM']),axis=1)
            PDM_T=np.concatenate((PDM_T , Thread_output[f'{thread.name}']['PDM_T']), axis=0)    

    PDM_Full=PDM +PDM_T

    return PDM_Full


def Distance_matrix_compute(Start_col,End_col,data,Nb_dim,Nb_lines,output):
    import numpy as np
    import dtw as dtw
    import threading

    import time
    start_time = time.time()  # Temps de départ
    print(f"{threading.current_thread().name} - start")
    #print("Start Distance_matrix_compute : Startcol= " +str(Start_col) +  "Endcol=  " +str(End_col) + " dim : " + str(Nb_dim) + " Nb_lines : " + str(Nb_lines))
    #print("data = " + str(data))
    
    Nb_col=End_col-Start_col
    Partial_dist_matrix = np.zeros((Nb_lines,Nb_col))
    Partial_dist_matrix_T = np.zeros((Nb_col,Nb_lines))
    #print("Parial dist_matrix dim : " + str(Nb_lines) +"(Nblines) " + str(Nb_col) + "Nbcol")
    for i in range(0,Nb_lines):
        for j in range(Start_col,End_col):   #Compute only half of the matrix as the distance is symetric !
            if j>=i:    
                
                line_i=(i)*Nb_dim
                line_j=(j)*Nb_dim

                query=data.iloc[line_i : line_i + Nb_dim ,:].values 
                template=data.iloc[line_j : line_j + Nb_dim ,:].values 
                #print("i= " +str(i) +  " j= " + str(j))
                #print("query" + str(query))
                #print("query" + str(template))

                Partial_dist_matrix[i, j-Start_col] = dtw.dtw(query, template,dist_method="euclidean",window_type='sakoechiba', distance_only=True).distance
                Partial_dist_matrix_T[j-Start_col,i] = Partial_dist_matrix[i, j-Start_col]
                #print("dist" + str(Partial_dist_matrix[i, j-Start_col]))

        if i*j/(Nb_lines*Nb_col) % 100 == 0:  # Mise à jour toutes les 100 itérations
            current_time = time.time() - start_time  # Temps écoulé jusqu'à présent
            time_per_iteration = current_time / (i + 1)  # Temps par itération
            remaining_time = time_per_iteration * (Nb_lines*Nb_col - i*j - 1)  # Temps restant
            print(f"{threading.current_thread().name} - Temps restant estimé: {remaining_time:.2f} secondes")


        dic_out={
                'PDM':Partial_dist_matrix,
                'PDM_T':Partial_dist_matrix_T
                }
    #print("PDM" + str(Partial_dist_matrix))
    #print("PDM_T" + str(Partial_dist_matrix_T))

    print(f"{threading.current_thread().name} - Completed: {time.time() - start_time:.2f} secondes")
    output[threading.current_thread().name] = dic_out
    return 



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



def plot_TS_clusters(Aggreg_Patients,Aggreg_parameters,filename_path,n_clusters):
    import numpy as np
    import matplotlib.pyplot as plt

    Timesteps=int(Aggreg_parameters['Stop_at_item'])-int(Aggreg_parameters['Start_at_item'])
    Nb_dim=int(Aggreg_Patients['Nb_dim'])

    param_dict = {}

    unique_rows = Aggreg_Patients['df'][['FT1','FV1','FT2','FV2']].drop_duplicates()
    unique_rows.reset_index()
    # Iterate through each row and print

    for index, row in unique_rows.iterrows():
        new_param = {
        'FT1': row['FT1'],
        'FV1': row['FV1'],
        'FT2': row['FT2'], 
        'FV2': row['FV2']
        }
        param_dict['dim_' + str(index+1)] = new_param


    id_x =np.linspace(0,len(Aggreg_Patients['df'].columns)-9,num=len(Aggreg_Patients['df'].columns)-9)

    #define subplot layout
    fig, ax = plt.subplots(n_clusters, Nb_dim, figsize=(40,48))
    fig.tight_layout()

    #fig.xlabel('Weeks') # to be linked with the aggregate function parameters
    #fig.ylabel('Actes ') # to be linked with the aggregate function parameters
    for i in range(n_clusters):
        for k in range(Nb_dim):
            # Filter data based on the 'clusters' column and transpose for plotting
            cluster_dataX = id_x
            
            #Filter the dataset by Cluster and dimension
            dim='dim_' + str(k+1)
            #print(dim + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'])
            #cluster_dataY = Aggreg_Patients['df'][Aggreg_Patients['df']['Cluster'] == i]
            cluster_dataY = Aggreg_Patients['df'][
                (Aggreg_Patients['df']['Cluster'] == i) &
                (Aggreg_Patients['df']['FV1'] == param_dict[dim]['FV1']) &
                (Aggreg_Patients['df']['FV2'] == param_dict[dim]['FV2'])
                ]
            cluster_dataY_trimmed  = cluster_dataY.iloc[:, 1:-8]

            #num_individuals = Aggreg_Patients['df']['Cluster'].value_counts()[i]
            num_individuals = cluster_dataY_trimmed.shape[0]

            # Plot on the respective axis
            #for j in range(len(cluster_dataY)):
            for j in range(num_individuals):
                if Nb_dim!=1:
                    ax[i,k].plot(cluster_dataX,cluster_dataY_trimmed.iloc[j,:])
                else:
                    ax[i].plot(cluster_dataX,cluster_dataY_trimmed.iloc[j,:])

            if k==Nb_dim-1 :
                if Nb_dim!=1:
                    ax[i,k].text(Timesteps+3, 0, 'N=' + str(num_individuals)) #correct this code with the dimension number.
                else:
                    ax[i].text(Timesteps+3, 0, 'N=' + str(num_individuals)) #correct this code with the dimension number.
            
            if i==n_clusters-1 :
                if Nb_dim!=1:
                    ax[0,k].set_title(dim + " " + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'], rotation=60)
                else:
                    ax[0].set_title(dim + " " + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'], rotation=60)
                #add here a text to explicit which dimension is shown.
            
            #plt.legend(loc='center right')  # Converted 'i' to string for the title
    
    plt.savefig(filename_path)
    return 