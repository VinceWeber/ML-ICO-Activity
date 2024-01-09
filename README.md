# ML-ICO-Activity

  From administraive hospital logs, try to cluster patient carepathes and predict global activity of services of hospital.

# Table of contents

- [Usage](#usage)
- [Installation](#installation)
- [Support](#Support)
- [Project status](#Project-status)
- [License](#license)


# Usage

[(Back to top)](#table-of-contents)

From a log of activity, stored in a SQL Database.
###0- Read a configuration file with parameters of the study
###1- Perform some data pretreatment in TSQL, filters,etc..
###2- Extract a dataset from the Database
###3- Run a aggregation query in TSQL to transform the log (1 line = 1 activity of a patient into a TimeserieTable : 1 line= 1 patient Time serie activity)
###4- Compute distance between several patients, by using DTW package (Giorgino. Computing and Visualizing Dynamic Time Warping Alignments).
###5- Perform a clustering
###6- Store result into mlflow.

# Installation

[(Back to top)](#table-of-contents)
  
  Please refer to requiremetns.txt to configure your environement.

# Project status

[(Back to top)](#table-of-contents)

    In progress, adding multidimensionnal clustering.
	
# License

[(Back to top)](#table-of-contents)
	
