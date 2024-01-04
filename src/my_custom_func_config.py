#python module dedicated fot MLFlow Configuration

def my_custom_func_MLFLOWconfig(Experiment_name,Experiment_tag1,Experiment_tag2):
    
    #Initialization of MLFlow UI
    import mlflow
    import subprocess
    # Split the command and its arguments into separate elements
    command = ['mlflow', 'server', '--host', '127.0.0.1', '--port', '8080']
    # Run the shell command in the background
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # LAUNCH ML FLOW UI
    mlflow.set_tracking_uri(uri="http://127.0.0.1:8080")

    # Set an experiment name, which must be unique and case-sensitive.
    experiment = mlflow.set_experiment(Experiment_name,None)
    

    # Launch a run into the experiment
    mlflow.end_run()
    mlflow.start_run()

    mlflow.set_experiment_tag(Experiment_tag1, Experiment_tag2)
    mlflow.set_tag(Experiment_tag1, Experiment_tag2)

    # Get Experiment Details
    print(f"Experiment_id: {experiment.experiment_id}")
    print(f"Artifact Location: {experiment.artifact_location}")
    print(f"Tags: {experiment.tags}")
    print(f"Lifecycle_stage: {experiment.lifecycle_stage}")

    return


def get_Create_dataset_parameters(CSV_config):

    DSprefix=CSV_config['DS_Prefix']
    Create_dataset_parameters={DSprefix + 'My_NIP_filter_1rst_date': CSV_config['My_NIP_filter_1rst_date'],
                                DSprefix + 'My_NIP_filter_2nd_date_delta_in_days': int(CSV_config['My_NIP_filter_2nd_date_delta_in_days']),
                                DSprefix + 'Site': str(CSV_config['Site']),
                                DSprefix + 'Start_Window_time': CSV_config['Start_Window_time'],
                                DSprefix + 'End_Window_time': CSV_config['End_Window_time'],
                                }

    return Create_dataset_parameters



def get_Aggreg_param(CSV_config):
    
    Aggprefix=CSV_config['Aggprefix']
    Aggreg_parameters={Aggprefix + 'Report_type': str(CSV_config['Report_type']),
                            Aggprefix + 'Aggreg_type': str(CSV_config['Aggreg_type']),
                            Aggprefix + 'Date_ref': str(CSV_config['Date_ref']),
                            Aggprefix + 'Start_at_item': str(CSV_config['Start_at_item']),
                            Aggprefix + 'Stop_at_item': str(CSV_config['Stop_at_item']),
                            Aggprefix + 'Method': str(CSV_config['Method']),
                            Aggprefix + 'Type_filter1': str(CSV_config['Type_filter1']),
                            Aggprefix + 'Val_filter1': str(CSV_config['Val_filter1']),
                            Aggprefix + 'Type_filter2': str(CSV_config['Type_filter2']),
                            Aggprefix + 'Val_filter2': str(CSV_config['Val_filter2']),
                            Aggprefix + 'Param_J0': str(CSV_config['Param_J0']),
                                }


    return Aggreg_parameters,Aggprefix


def set_Time_clust_parameters(CSV_Clust_config):
    
    if CSV_Clust_config['T_Nb_clusters']!='None':
        default_n_clust=CSV_Clust_config['T_Nb_clusters']
    else:
        default_n_clust=None

    #DIMENSION NAME
    clust_name = CSV_Clust_config['T_Clust_Name']
    image_format = ".png"

    #Filename
    mypath= CSV_Clust_config['myouputpath']
    curve_filename= mypath + clust_name + "_curve" + image_format
    curve_mlflowname= clust_name +"_curve"

    PCA_filename = mypath + clust_name + "_PCA" + image_format
    PCA_mlflowname= clust_name + "_PCA"

    Cluster_summary_filename = mypath + clust_name + "_Clust_Summary.csv"
    Cluster_summary_mlflowname = clust_name + "_Clust_Summary"

    NIP_Carac_filename = mypath + clust_name + "_NIP_Carac.csv"
    NIP_Carac_mlflowname = clust_name + "_NIP_Carac"

    Table_name= CSV_Clust_config['T_Table_Name']

    #mlflowoutput
    Time_Clust_parameters={
        'Method': CSV_Clust_config['T_Method'],
        'Nb_clusters' : default_n_clust,
        'max_nb_clusters' : CSV_Clust_config['T_max_nb_clusters'],
        'threshold': CSV_Clust_config['T_threshold'],
        'clust_name' : clust_name,
        'curve_filename' : curve_filename ,
        'curve_mlflowname' : curve_mlflowname, 
        'PCA_filename' : PCA_filename , 
        'PCA_mlflowname' : PCA_mlflowname,
        'Summary_filename' : Cluster_summary_filename,
        'Summary_mlflowname' : Cluster_summary_mlflowname,
        'NIP_Carac_filename' : NIP_Carac_filename,
        'NIP_Carac_mlflowname' : NIP_Carac_mlflowname,
        'Table_name' : Table_name
        }

    return Time_Clust_parameters

def set_parcours_clust_parameters(CSV_Clust_config):
    
    if CSV_Clust_config['P_Nb_clusters']!='None':
        default_n_clust=CSV_Clust_config['P_Nb_clusters']
    else:
        default_n_clust=None

    #DIMENSION NAME
    clust_name = CSV_Clust_config['P_Clust_Name']
    image_format = ".png"

    #Filename
    mypath= CSV_Clust_config['myouputpath']
    curve_filename= mypath + clust_name + "_curve" + image_format
    curve_mlflowname= clust_name +"_curve"

    PCA_filename = mypath + clust_name + "_PCA" + image_format
    PCA_mlflowname= clust_name + "_PCA"

    Cluster_summary_filename = mypath + clust_name + "_Clust_Summary.csv"
    Cluster_summary_mlflowname = clust_name + "_Clust_Summary"

    NIP_Carac_filename = mypath + clust_name + "_NIP_Carac.csv"
    NIP_Carac_mlflowname = clust_name + "_NIP_Carac"

    Table_name= CSV_Clust_config['P_Table_Name']

    #mlflowoutput
    Parcours_Clust_parameters={
        'Method': CSV_Clust_config['P_Method'],
        'Nb_clusters' : default_n_clust,
        'max_nb_clusters' : CSV_Clust_config['P_max_nb_clusters'],
        'threshold': CSV_Clust_config['P_threshold'],
        'clust_name' : clust_name,
        'curve_filename' : curve_filename ,
        'curve_mlflowname' : curve_mlflowname, 
        'PCA_filename' : PCA_filename , 
        'PCA_mlflowname' : PCA_mlflowname,
        'Summary_filename' : Cluster_summary_filename,
        'Summary_mlflowname' : Cluster_summary_mlflowname,
        'NIP_Carac_filename' : NIP_Carac_filename,
        'NIP_Carac_mlflowname' : NIP_Carac_mlflowname,
        'Table_name' : Table_name
        }

    return Parcours_Clust_parameters

def get_dtw_param(CSV_config):

    dtw_param={
    'dist_method': CSV_config['P_dist_method'],
    'window_type': CSV_config['P_Window_type'],
    'window_args': None,
    'distance_only':True
    }

    if CSV_config['P_Window_type']!='None':
        dtw_param['window_args'] = {'window_size': int(CSV_config['P_Window_size'])}
    else:
        dtw_param['window_type'] = None

    return dtw_param


def set_CPP_Plot_parameters(CSV_Clust_config):
    
    CPP_plot=CSV_Clust_config['CPP_Plot']
    CPP_order=CSV_Clust_config['CPP_Order']
    CPP_Table_Name=CSV_Clust_config['CPP_Save_Tble_Name']
    CPP_Requete=CSV_Clust_config['CPP_Requete']

    CPP_Filter_df_col=CSV_Clust_config['CPP_Filter_df_col']
    CPP_Filter_df_value=CSV_Clust_config['CPP_Filter_df_value']

    primary_clust_name=CSV_Clust_config['CPP_Clust1_name']
    primary_clust_TableName=CSV_Clust_config['CPP_Clust1_T_name']

    if CSV_Clust_config['CPP_Clust2_name']!='None':
        sub_clust_name=CSV_Clust_config['CPP_Clust2_name']
        sub_clust_TableName=CSV_Clust_config['CPP_Clust2_T_name']
    else:
        sub_clust_name=None
        sub_clust_TableName=None

    #mlflowoutput
    CPP_parameters={
        'CPP_plot_Bool': CPP_plot,
        'CPP_order' : CPP_order,
        'CPP_Table_Name': CPP_Table_Name,
        'CPP_Requete':CPP_Requete,
        'CPP_Filter_df_col':CPP_Filter_df_col,
        'CPP_Filter_df_value':CPP_Filter_df_value,
        'primary_clust_name':primary_clust_name,
        'primary_clust_TableName':primary_clust_TableName,
        'sub_clust_name':sub_clust_name,
        'sub_clust_TableName':sub_clust_TableName,

        }
    return CPP_parameters


def set_FPP_Plot_parameters(CSV_Clust_config):
    
    FPP_plot=CSV_Clust_config['FPP_Plot']
    FPP_order=CSV_Clust_config['FPP_Order']
    FPP_Table_Name=CSV_Clust_config['FPP_Save_Tble_Name']
    FPP_Requete=CSV_Clust_config['FPP_Requete']

    FPP_Filter_df_col=CSV_Clust_config['FPP_Filter_df_col']
    FPP_Filter_df_value=CSV_Clust_config['FPP_Filter_df_value']

    clust1Name=CSV_Clust_config['FPP_Clust1_name']
    clust1TableName=CSV_Clust_config['FPP_Clust1_T_name']

    if CSV_Clust_config['FPP_Clust2_name']!='None':
        clust2Name=CSV_Clust_config['FPP_Clust2_name']
        clust2TableName=CSV_Clust_config['FPP_Clust2_T_name']
    else:
        clust2Name=None
        clust2TableName=None

    if CSV_Clust_config['FPP_Clust3_name']!='None':
        clust2Name=CSV_Clust_config['FPP_Clust3_name']
        clust2TableName=CSV_Clust_config['FPP_Clust3_T_name']
    else:
        clust3Name=None
        clust3TableName=None


    #mlflowoutput
    FPP_parameters={
        'FPP_plot_Bool': FPP_plot,
        'FPP_order' : FPP_order,
        'FPP_Table_Name': FPP_Table_Name,
        'FPP_Requete':FPP_Requete,
        'FPP_Filter_df_col':FPP_Filter_df_col,
        'FPP_Filter_df_value':FPP_Filter_df_value,
        'clust1Name':clust1Name,
        'clust1TableName':clust1TableName,
        'clust2Name':clust2Name,
        'clust2TableName':clust2TableName,
        'clust3Name':clust3Name,
        'clust3TableName':clust3TableName
        }
    return FPP_parameters
