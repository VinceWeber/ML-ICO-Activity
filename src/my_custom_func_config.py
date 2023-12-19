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
    experiment = mlflow.set_experiment(Experiment_name)
    mlflow.set_experiment_tag(Experiment_tag1, Experiment_tag2)

    # Get Experiment Details
    print(f"Experiment_id: {experiment.experiment_id}")
    print(f"Artifact Location: {experiment.artifact_location}")
    print(f"Tags: {experiment.tags}")
    print(f"Lifecycle_stage: {experiment.lifecycle_stage}")

    # Launch a run into the experiment
    mlflow.end_run()
    mlflow.start_run()

    return


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
        'NIP_Carac_mlflowname' : NIP_Carac_mlflowname
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
        'NIP_Carac_mlflowname' : NIP_Carac_mlflowname
        }

    return Parcours_Clust_parameters

def get_dtw_param(CSV_config):

    dtw_param={
    'dist_method': CSV_config['P_dist_method'],
    'window_type': CSV_config['P_Window_type'],
    'window_args': None,
    'distance_only':True
    }

    if CSV_config['P_Window_size']!='None':
        dtw_param['window_args'] = {'window_size': int(CSV_config['P_Window_size'])}

    return dtw_param