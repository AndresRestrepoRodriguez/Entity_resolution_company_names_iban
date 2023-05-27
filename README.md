# Entity_resolution_company_names_iban

The purpose of this repository is to apply entity resolution, based on company names and IBANs.

## Approach
To solve this problem, the following steps were applied:
### Duplicate Removal    
1. The input CSV with the data is read
2. Missing and invalid values are removed
3. The company name is normalized, therefore, it is converted to lower case, punctuation marks and extra white spaces are removed
4. Finally, duplicate values are removed from the IBAN and normalized name columns
5. The results are saved in a CSV file

### Entity Resolution
1. The data resulting from the duplicate removal stage is taken as input
2. The normalized names of each company are concatenated with their respective non-normalized IBAN
3. Terms associated with the business are removed, such as Corp, GmbH, among others. This, with the cleanco package
4. The IBAN is normalized by removing the last 2 digits
5. The names without business terms and the normalized IBAN are concatenated
6. The unique values of the previous concatenation are obtained
7. The Levenshtein distance is applied between each record of step 2 and each of the values obtained in step 6. Assigning to each record the company to which it is associated. The above, through the fuzzywuzzy package
8. A grouping function is applied by the assigned company, to obtain the list of IBANS and associated names
8. The results are saved in a CSV file

## Code Overview
The repository has two python scripts.
- pipeline.py: It contains the necessary functions to apply the aforementioned approach. This script receives the following input parameters.
    - '-f' or '--input_file': Path input file (CSV)
    - '-o' or '--output_path': Folder path where the results will be saved 
- config.py: Contains the definition of names of columns and variables used to keep an order.

Additionally, it has the data folder, where the test csv file is located.

## Usage
These instructions are specified for Linux OS.

Create a virtual python environment
```
python3 -m venv env
```

Activate the created virtual environment
```
source env/bin/activate
```

Install the necessary packages
```
pip install -r requirements.txt
```

Clone the repository
```
git clone https://github.com/AndresRestrepoRodriguez/Entity_resolution_company_names_iban.git
```

Go to the repository folder
```
cd Entity_resolution_company_names_iban
```

Run the script pipeline.py
```
python3 pipeline.py -f 'data/source.csv' -o 'data/'
```

Review the generated results in the output folder. Two files should have been created.
- duplicate_removal_output.csv
- entity_resolution_output.csv

If the files already exist, they will be overwritten.
