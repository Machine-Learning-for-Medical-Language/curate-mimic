# curate-mimic - A repository to guide in demonstrating how to curate the MIMIC III database with Natural Language Processing

0. Preliminary steps:
  a. Obtain access to the MIMIC III database (ask your PI).
  b. Obtain a UMLS account and API key
  c. Install Docker and docker-compose

1. Setup cTAKES containers:
  a. ```git clone git@github.com:Machine-Learning-for-Medical-Language/ctakes-rest-package.git```
  b. ```cd ctakes-rest-package```
  c. ```export umls_api_key=<api key from 0.b>```
  d. ```docker-compose up -d --scale ctakes=N```   # This starts N containers -- each requires around 4 GB RAM.

2. Run the python script to process the data -- run with -h to receive detailed documentation of the options:
  a. ```python process_mimic.py --input-path <path to NOTEEVENTS.csv file> --output-format <json|mongo|xmi|fhir> --output-args <arg1=val1,arg2=val2,...>```

3. For MongoDB usage, setup a few indices for faster querying:
  a. TODO

