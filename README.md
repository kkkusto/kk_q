README: Script Execution Workflow
Overview
This document outlines the steps and modules involved in executing scripts for managing file processing and ingestion tasks. The workflow is divided into two major steps, each containing specific modules with clearly defined purposes, testing environments, and results.

Steps and Modules
Step 1: File Processing from PNP Path
Extract filenames from PNP path

Script: RDRetrigger1_pnp.sh
Module: Extract file names for processing.
Script Type: JavaScript
Server: PNP
Testing: QA
Script Name: RdAutomation
Status: Done
Comments: Successfully tested and executed.
Get successful filenames from Oracle

Identifies and retrieves the processed filenames from the Oracle database.
Identify missing files

Compares the filenames from PNP with the Oracle database and lists the missing files.
Rename missing files

Renames files based on predefined naming conventions.
Put missing filenames in Oracle

Updates the Oracle database with the missing filenames.
Step 2: Manual Ingestion Path and Restoration
Copy to manual ingestion path

Script: Shell script
Server: NAS
Testing: NAS Commands
Script Name: execute_my_script
Status: Done
Comments: Successfully run on NAS.
Set src_path to manual ingestion path

Script: RDRetrigger2_prod.sh
Module: Configure source paths for ingestion.
Script Type: JavaScript
Server: Prod
Testing: QA
Script Name: IOParameterUpdater
Status: Done
Comments: Configurations successfully updated.
Put filenames in rd_input_feeds.txt

Module: Write file paths to the input feeds file.
Run ingestion job

Executes the ingestion process for the files in rd_input_feeds.txt.
Restore src_path path

Script: RDRetrigger2_prod.sh
Module: Restore original source paths post-ingestion.
Script Type: JavaScript
Server: Prod
Testing: QA
Script Name: IOParameterRestore
Status: Done
Comments: Source paths successfully restored.
Testing and Execution
All scripts have been tested in QA environments and verified for functionality.
Commands requiring execution on NAS were run and validated successfully.
Dependencies
JavaScript Runtime: Ensure the JavaScript runtime environment is installed and configured on the servers.
Shell Scripting: Requires a UNIX/Linux environment to run shell scripts.
Oracle Database Access: Scripts depend on the Oracle database for reading and updating filenames.
How to Use
Place the scripts (RDRetrigger1_pnp.sh and RDRetrigger2_prod.sh) in the appropriate directories as per the server environments.
Follow the workflow as outlined in the Steps and Modules section.
Use NAS commands for tasks requiring manual intervention.
Verify the output logs for successful execution.
Contributors
This process and scripts were developed and tested by the project team.
Detailed Documentation Template
1. Objective
Provide a detailed description of the workflow's purpose, e.g., managing file processing, tracking missing files, and automating ingestion tasks.

2. Prerequisites
Required software installations (e.g., JavaScript runtime, shell access).
Database access credentials and permissions.
3. Step-by-Step Instructions
Step 1: File Processing

Detailed explanation of extracting, identifying, and processing files.
Step 2: Manual Ingestion

Include NAS command examples and paths to configure.
4. Script Descriptions
Include each script's purpose, inputs, outputs, and parameters.
5. Testing and Debugging
Steps for verifying successful execution.
Common issues and troubleshooting tips.
6. Maintenance
Instructions for updating scripts or configurations.



README: Script Execution Workflow
Overview
This document outlines the steps and modules involved in executing scripts for managing file processing and ingestion tasks. The workflow is divided into two major steps, each containing specific modules with clearly defined purposes, testing environments, and results.

Steps and Modules
Step 1: File Processing from PNP Path
Extract filenames from PNP path

Script: RDRetrigger1_pnp.sh
Module: Extract file names for processing.
Script Type: JavaScript
Server: PNP
Testing: QA
Script Name: RdAutomation
Status: Done
Comments: Successfully tested and executed.
Get successful filenames from Oracle

Identifies and retrieves the processed filenames from the Oracle database.
Identify missing files

Compares the filenames from PNP with the Oracle database and lists the missing files.
Rename missing files

Renames files based on predefined naming conventions.
Put missing filenames in Oracle

Updates the Oracle database with the missing filenames.
Step 2: Manual Ingestion Path and Restoration
Copy to manual ingestion path

Script: Shell script
Server: NAS
Testing: NAS Commands
Script Name: execute_my_script
Status: Done
Comments: Successfully run on NAS.
Set src_path to manual ingestion path

Script: RDRetrigger2_prod.sh
Module: Configure source paths for ingestion.
Script Type: JavaScript
Server: Prod
Testing: QA
Script Name: IOParameterUpdater
Status: Done
Comments: Configurations successfully updated.
Put filenames in rd_input_feeds.txt

Module: Write file paths to the input feeds file.
Run ingestion job

Executes the ingestion process for the files in rd_input_feeds.txt.
Restore src_path path

Script: RDRetrigger2_prod.sh
Module: Restore original source paths post-ingestion.
Script Type: JavaScript
Server: Prod
Testing: QA
Script Name: IOParameterRestore
Status: Done
Comments: Source paths successfully restored.
Steps to Run
Pre-Requisites
Ensure Java is Installed: Confirm javac and java commands are available.
Ensure Shell Access: Verify access to the PNP, NAS, and Prod servers.
Database Access: Confirm Oracle database credentials are available and configured.
Scripts and Files: Ensure all scripts (RDRetrigger1_pnp.sh and RDRetrigger2_prod.sh) and Java files are present on the respective servers.
Compilation
Navigate to the directory containing the Java files.
Compile all Java files:
bash
Copy code
javac *.java
Execution
Step 1: Run on PNP Server

Navigate to the PNP server.
Execute the first script:
bash
Copy code
sh RDRetrigger1_pnp.sh
Verify logs to ensure successful execution.
Step 2: Run on Prod Server

Navigate to the Prod server (01 environment).
Execute the second script:
bash
Copy code
sh RDRetrigger2_prod.sh
Verify logs to confirm all ingestion tasks completed successfully.
Testing and Execution
All scripts have been tested in QA environments and verified for functionality.
Commands requiring execution on NAS were run and validated successfully.
Dependencies
Java Runtime Environment: Ensure Java runtime is installed and configured.
Shell Scripting: Requires a UNIX/Linux environment to run shell scripts.
Oracle Database Access: Scripts depend on the Oracle database for reading and updating filenames.
