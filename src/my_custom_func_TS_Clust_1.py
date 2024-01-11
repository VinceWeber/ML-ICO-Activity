

#Definition d'une fonction de création de la requete d'aggregation
def req_aggreg (Aggreg_parameters,Aggprefix):
    
    Requete="""EXECUTE [dbo].[ReportCarePathActivtiy_By_actes] """ \
    + Aggreg_parameters[Aggprefix + 'Report_type'] + "," \
		+ "'" + Aggreg_parameters[Aggprefix +'Aggreg_type'] + "'"+ "," \
			+ Aggreg_parameters[Aggprefix +'Date_ref'] + "," \
				+ Aggreg_parameters[Aggprefix +'Start_at_item'] + "," \
					+ Aggreg_parameters[Aggprefix +'Stop_at_item'] + "," \
						+ "'" + Aggreg_parameters[Aggprefix +'Method'] + "'" + "," \
							+ Aggreg_parameters[Aggprefix +'Type_filter1'] + "," \
								+ "'" + Aggreg_parameters[Aggprefix +'Val_filter1'] + "'" + "," \
									+ Aggreg_parameters[Aggprefix +'Type_filter2'] + "," \
										+ "'" + Aggreg_parameters[Aggprefix +'Val_filter2'] + "'" + "," \
											+ "'" + Aggreg_parameters[Aggprefix +'Param_J0'] + "'"

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


def Create_dataset (Create_dataset_parameters,DSprefix):
    from datetime import datetime
    from datetime import timedelta
    import pandas as pd
    import Parcours_Classes as PC

    
    My_NIP_filter_1rst_date=datetime.strptime(Create_dataset_parameters[DSprefix + 'My_NIP_filter_1rst_date'], '%m-%d-%Y %H:%M:%S')
    My_NIP_filter_2nd_date=My_NIP_filter_1rst_date + timedelta(days=Create_dataset_parameters[DSprefix + 'My_NIP_filter_2nd_date_delta_in_days'])

    Site=Create_dataset_parameters[DSprefix + 'Site']
    filtreNIP=Create_dataset_parameters[DSprefix + 'FiltreNIP']

    Mydataset_date1=datetime.strptime(Create_dataset_parameters[DSprefix + 'Start_Window_time'], '%m-%d-%Y %H:%M:%S')
    Mydataset_date2=datetime.strptime(Create_dataset_parameters[DSprefix + 'End_Window_time'], '%m-%d-%Y %H:%M:%S')

    Caract_Df_SH = pd.DataFrame.from_dict(PC.Caracteristiques_Dataset_Parcours(1, My_NIP_filter_1rst_date,My_NIP_filter_2nd_date,Site,Mydataset_date1,Mydataset_date2,filtreNIP).get_x())

    #print(Caract_Df_SH)
    return Caract_Df_SH


def plot_carepath(dataset,filename_path,mlflow,mlflowname):
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
    if mlflow!=None:
        mlflow.log_artifact(filename_path, mlflowname)
        plt.savefig(filename_path)
    else:
        plt.show()
    
    del Temp_dataset
    return #Myscatterplot

def chk_Agg_param(Agg_param):
    if Agg_param!=None:
        result= True
    else:
        result= False
    return result

def old_get_Aggreg_Dataset(Agg_param1,Agg_param2=None,Agg_param3=None,Agg_param4=None,Agg_param5=None):
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

def get_Aggreg_Dataset2(list_param,Aggprefix):
    import pandas as pd
    import Sql_Alchemy_Classes as AlSQL

    agg_list = list_param
    print(f"Number of aggregation dimensions: {len(agg_list)}")
    
    df_out=pd.DataFrame()

    for Agg_param in agg_list: 
        
        #definition de la requete
        Requete=req_aggreg (Agg_param,Aggprefix)    

        #Get the dataset
        df=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,True)
        
        df.replace('','0',inplace=True)
        for col in df.columns[1:]:  # Starting from the second column onwards
            df[col] = pd.to_numeric(df[col], errors='coerce')

        #Add information of the parameters used
        df['FT1']=Agg_param[Aggprefix + 'Type_filter1']
        df['FV1']=Agg_param[Aggprefix + 'Val_filter1']
        df['FT2']=Agg_param[Aggprefix + 'Type_filter2']
        df['FV2']=Agg_param[Aggprefix + 'Val_filter2']


        #Merge this newdataset with the previous one
        df_out = pd.concat([df_out, df], axis=0)
    
        dict_out={
            'Nb_dim': str(len(agg_list)),
            'df': df_out
        }
    return dict_out

def get_Aggreg_Dataset3(Param_dict,Aggprefix):
    import pandas as pd
    import Sql_Alchemy_Classes as AlSQL

    nb_dim_key=Aggprefix + 'Nb_dim'
    nb_dim=Param_dict[nb_dim_key]
    print(f"Number of aggregation dimensions: {nb_dim}")
    
    df_out=pd.DataFrame()
    Agg_param=Param_dict

    for dim in range(nb_dim): 
        
        dim_key = f"Dim{dim+1}"
        if dim_key in Param_dict:
            Agg_param[f"{Aggprefix + 'Type_filter1'}"]=Param_dict[dim_key][f"{Aggprefix + 'Type_filter1'}"]
            Agg_param[f"{Aggprefix + 'Val_filter1'}"]=Param_dict[dim_key][f"{Aggprefix + 'Val_filter1'}"]
            Agg_param[f"{Aggprefix + 'Type_filter2'}"]=Param_dict[dim_key][f"{Aggprefix + 'Type_filter2'}"]
            Agg_param[f"{Aggprefix + 'Val_filter2'}"]=Param_dict[dim_key][f"{Aggprefix + 'Val_filter2'}"]

        #definition de la requete
        Requete=req_aggreg (Agg_param,Aggprefix)    

        #Get the dataset
        df=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,True)
        
        df.replace('','0',inplace=True)
        for col in df.columns[1:]:  # Starting from the second column onwards
            df[col] = pd.to_numeric(df[col], errors='coerce')

        #Add information of the parameters used
        df['FT1']=Agg_param[Aggprefix + 'Type_filter1']
        df['FV1']=Agg_param[Aggprefix + 'Val_filter1']
        df['FT2']=Agg_param[Aggprefix + 'Type_filter2']
        df['FV2']=Agg_param[Aggprefix + 'Val_filter2']


        #Merge this newdataset with the previous one
        df_out = pd.concat([df_out, df], axis=0)
    
        dict_out={
            'Nb_dim': str(nb_dim),
            'df': df_out
        }
    return dict_out


def GetDistanceMatrix(Parcours_dict, Aggreg_parameters,Aggprefix,dtw_param):
    import numpy as np
    import dtw as dtw

    my_dist_method=dtw_param['dist_method']  # 'euclidian'
    my_window_type=dtw_param['window_type']  # 'sakoechiba'
    my_window_args=dtw_param['window_args']  # 'sakoechiba'
    my_distance_only=dtw_param['distance_only']  # 'sakoechiba'

    Timesteps=int(Aggreg_parameters[Aggprefix + 'Stop_at_item'])-int(Aggreg_parameters[Aggprefix + 'Start_at_item'])
    Parcours_dict['df'].sort_values(['NIP', 'FV1','FV2'], ascending=[True, True, True], inplace=True)

    data_to_plot = Parcours_dict['df'].iloc[:,1:(Timesteps+2)] 
    newColumnName='Start'
    data_to_plot.insert(0,newColumnName,0) # Add an empty column to avoid a not understanding behaviour of the distance computation

    Nb_NIP=len(Parcours_dict['df']['NIP'].unique())
    Nb_dim=int(Parcours_dict['Nb_dim'])
    result = np.zeros((Nb_NIP, Nb_NIP))

    import time
    start_time = time.time()  # Temps de départ

    for i in range(Nb_NIP):
        for j in range(i,Nb_NIP):   #Compute only half of the matrix as the distance is symetric !
            
            line_i=i*Nb_dim
            line_j=j*Nb_dim

            query=data_to_plot.iloc[line_i : line_i + Nb_dim ,:].values # For one dimension Time serie
            template=data_to_plot.iloc[line_j : line_j + Nb_dim ,:].values # For one dimension Time serie
            #print("i= " +str(i) +  " j= " + str(j))
            #print("query" + str(query))
            #print("query" + str(template))

            #result[i, j] = dtw.dtw(query, template, distance_only=True).distance
            result[i, j] =dtw.dtw(query, template,dist_method=my_dist_method,window_type=my_window_type, window_args=my_window_args,distance_only=my_distance_only).distance
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



def plot_TS_clusters(Aggreg_Table,Timesteps,filename_path,cluster_dict,mlflow,mlflowname):
    #design to plot Time series curves and return a plot
    # Input parameters:
    # Aggreg_Patients : Table with the activity aggregated by timesteps (in column)
    # and one line per individual
    # it can also work with multidimensional time series, then each individual is a group of nlines (n = nb of dimensions)
    
    import numpy as np
    import matplotlib.pyplot as plt

    n_clusters = cluster_dict['nb_cluster']
    colname = cluster_dict['Column_name']

    Timesteps
    X_text=Timesteps-0.5
    Y_text_base=0.8 #is multiplied later by the max value of the subdataset
    
    Nb_dim=int(Aggreg_Table['Nb_dim'])

    param_dict = {}

    #print("CHECK plot_TS_clusters FUNCTION")
    #print("Aggreg_Table")
    #print(Aggreg_Table['df'])


    unique_rows = Aggreg_Table['df'][['FT1','FV1','FT2','FV2']].drop_duplicates()
    unique_rows.reset_index()

    #print("unique_rows")
    #print(unique_rows)

    # Iterate through each row and print

    for index, row in unique_rows.iterrows():
        new_param = {
        'FT1': row['FT1'],
        'FV1': row['FV1'],
        'FT2': row['FT2'], 
        'FV2': row['FV2']
        }
        param_dict['dim_' + str(index+1)] = new_param

    #ADD HERE A CHECK THAT NB OF DIMENSIONS GETTING FROM PARAM_DICT IS COHERENT WITH THE INITIAL PARAMETERS
    #TO AVOID THE CASE TO HAVE REDUNDANT FILTER IN THE CSV CONFIGURATION FILES WHICH WILL FAIL THIS PROCEDURE

    #print("param_dict")
    #print(param_dict)

    #id_x =np.linspace(0,len(Aggreg_Patients['df'].columns)-9,num=len(Aggreg_Patients['df'].columns)-9)
    id_x = np.linspace(0, Timesteps, num=Timesteps+1)

    #define subplot layout
    fig, ax = plt.subplots(n_clusters, Nb_dim, figsize=(15,30))
    fig.tight_layout()

    #fig.xlabel('Weeks') # to be linked with the aggregate function parameters
    #fig.ylabel('Actes ') # to be linked with the aggregate function parameters
    for i in range(n_clusters):  #Check if nb_cluster=1
        for k in range(Nb_dim):
            # Filter data based on the 'clusters' column and transpose for plotting
            cluster_dataX = id_x
            
            #Filter the dataset by Cluster and dimension
            dim='dim_' + str(k+1)
            #print(dim + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'])
            #cluster_dataY = Aggreg_Patients['df'][Aggreg_Patients['df']['Cluster'] == i]

            #print("DEBUG")
            #print("""Aggreg_Table['df'][
            #    (Aggreg_Table['df'][colname] == i) &
            #    (Aggreg_Table['df']['FV1'] == param_dict[dim]['FV1']) &
            #    (Aggreg_Table['df']['FV2'] == param_dict[dim]['FV2'])
            #    ]""")
            #print(Aggreg_Table['df'][
            #    (Aggreg_Table['df'][colname] == i) &
            #    (Aggreg_Table['df']['FV1'] == param_dict[dim]['FV1']) &
            #    (Aggreg_Table['df']['FV2'] == param_dict[dim]['FV2'])
            #    ])

            cluster_dataY = Aggreg_Table['df'][
                (Aggreg_Table['df'][colname] == i) &
                (Aggreg_Table['df']['FV1'] == param_dict[dim]['FV1']) &
                (Aggreg_Table['df']['FV2'] == param_dict[dim]['FV2'])
                ]
            cluster_dataY_trimmed  = cluster_dataY.iloc[:, 1:(Timesteps+2)]
            cluster_dataY_Mean_ind = cluster_dataY[['NIP',colname + '_Mean_Indiv']]

            Y_text=cluster_dataY_trimmed.max().max()*Y_text_base
            
            
            #num_individuals = Aggreg_Patients['df']['Cluster'].value_counts()[i]
            num_individuals = cluster_dataY_trimmed.shape[0]

            # Plot on the respective axis
            #for j in range(len(cluster_dataY)):
            for j in range(num_individuals):
                
                color=None
                if cluster_dataY_Mean_ind.iloc[j,1] :  # Mean or Avg Individual -> linewidth=10, 1 else
                    width=5
                    mytext=True
                    color='blue'
                else:
                    width=1
                    mytext=False

                if 'Avg' in cluster_dataY_Mean_ind.iloc[j, 0]:
                    linestyle = '-.'  # Dot-dash line
                    y_pos = 1
                    color='red'
                else:
                    linestyle = '-'  # Continuous line
                    y_pos = 2
                
                if Nb_dim!=1:   # if Nb_dim = 2 or more ( !! Zero value can be an error ! )
                    if n_clusters==1:
                        line = ax[k].plot(cluster_dataX,cluster_dataY_trimmed.iloc[j,:], linewidth=width, linestyle=linestyle, color=color)
                        #color = line[0].get_color()
                        if mytext:
                            ax[k].text(X_text, y_pos,str(cluster_dataY_Mean_ind.iloc[j,0]), color=color)
                    else:
                        line = ax[i,k].plot(cluster_dataX,cluster_dataY_trimmed.iloc[j,:], linewidth=width, linestyle=linestyle, color=color)
                        #color = line[0].get_color()
                        if mytext:
                            ax[i,k].text(X_text, y_pos,str(cluster_dataY_Mean_ind.iloc[j,0]), color=color)
                
                else:       #if Nb_dim =1
                    if n_clusters==1:
                        line = ax.plot(cluster_dataX,cluster_dataY_trimmed.iloc[j,:], linewidth=width, linestyle=linestyle, color=color)
                        if mytext:
                            ax.text(X_text, Y_text + y_pos,str(cluster_dataY_Mean_ind.iloc[j,0]), color=color)
                    else:
                        line = ax[i].plot(cluster_dataX,cluster_dataY_trimmed.iloc[j,:], linewidth=width, linestyle=linestyle, color=color)
                        color = line[0].get_color()
                        if mytext:
                            ax[i].text(X_text, Y_text + y_pos,str(cluster_dataY_Mean_ind.iloc[j,0]), color=color)
                    
            if k==Nb_dim-1 :
                if Nb_dim!=1:
                    if n_clusters==1:
                        ax[k].text(X_text, Y_text, 'N=' + str(num_individuals)) #correct this code with the dimension number.
                    else:
                        ax[i,k].text(X_text, Y_text, 'N=' + str(num_individuals)) #correct this code with the dimension number.
                else:
                    if n_clusters==1:
                        ax.text(X_text, Y_text, 'N=' + str(num_individuals)) #correct this code with the dimension number.
                    else:
                        ax[i].text(X_text, Y_text, 'N=' + str(num_individuals)) #correct this code with the dimension number.
            
            if i==n_clusters-1 :
                if Nb_dim!=1:
                    if n_clusters==1:
                        ax[k].set_title(dim + " " + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'], rotation=0)
                    else:
                        ax[0,k].set_title(dim + " " + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'], rotation=0)
                
                else:
                    if n_clusters==1:
                        ax.set_title(dim + " " + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'], rotation=0)
                    else:
                        ax[0].set_title(dim + " " + param_dict[dim]['FV1'] + " & " + param_dict[dim]['FV2'], rotation=0)
                #add here a text to explicit which dimension is shown.
            
            #plt.legend(loc='center right')  # Converted 'i' to string for the title
    
    plt.savefig(filename_path)
    mlflow.log_artifact(filename_path, mlflowname)
    plt.close()

    return 