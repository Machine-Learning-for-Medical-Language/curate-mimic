# curate-mimic - A repository to guide in demonstrating how to curate the MIMIC III database with Natural Language Processing

0. Preliminary steps: 
    * Obtain access to the MIMIC III database (ask your PI).
    * Obtain a UMLS account and API key
    * Install Docker and docker-compose
    * If writing to a MongoDB database:
      * Create local directory: ```mkdir mimic_db```
      * docker run --rm --name mongodb -d -v mimic_db:/data/db -p 27017:27017 mongo

1. Setup cTAKES containers: 
    * ```git clone git@github.com:Machine-Learning-for-Medical-Language/ctakes-rest-package.git```
    * ```cd ctakes-rest-package```
    * ```export umls_api_key=<api key from above>```
    * ```docker-compose up -d --scale ctakes=N```   # This starts N containers -- each requires around 4 GB RAM.

2. Run the python script to process the data -- run with -h to receive detailed documentation of the options:
    * ```python process_mimic.py --input-path <path to NOTEEVENTS.csv file> --output-format <json|mongo|xmi|fhir> --output_dir <directory to write files if output format is file-based>```
    * If you run with the flag ```--max-notes N```, you can run on a subset to make sure everything is working correctly before processing the whole dataset.

3. If you used MongoDB, check into some of the output:

    ```$ mongo```

    ```> use mimic```

    ```> db.note_nlp.stats()['count']``` # Should return number of notes processed and entered into database

    



