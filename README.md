# Companies House Data Fetch CLI

CLI tool with two functions:

1) Connecting to a MySQL database, and extracting data from HM Land Registry
property ownership datasets.

2) Querying the Companies House API to enrich data about companies appearing in the
property ownership database, especially beneficial owners of foreign companies and
charge holder information over UK companies.

## Goals

- To centralise the extraction and enrichment of company data appearing in HM Land Registry property ownership datasets, and make structured queries against such data.

- Interface with a MySQL instance to persist company data. 

- To enable deeper analyse of company beneficial ownership networks and UK property asset ownership positions, and a temporal view of property portfolios.

- Maximise data fetching efficiency by leverage multiple processes and threads. Given the Global Interpreter Lock problem, this tool leverages new process creation to multiply the number of API calls which can be made.

## Disclaimer

I created this CLI-tool solely as a hobby project to be able work on a somewhat significant project with Python MySQL, and other skills/tools. 

I don't recommend that anyone uses this tool for any professional activities, as 
I cannot guarantee it meets any professional standards for such tooling.

As such, I don't give any warranty as to the efficacy of this tool, and users 
must be aware that they are using it at their own risk.
 
## Demo Video

- Extracting dataset data
- Collecting foreign beneficial owner data
- Collecting charge data

## Pre-requisites 

You will need at least one Companies House API key, although it is recommended to 
use no more than three keys to maximise output.

The CLI requires a .env file located at the root of the project with the following
variables:
    - MYSQL_HOST=""
    - MYSQL_USER=""
    - MYSQL_PASSWORD=""
    - MYSQL_D=""
    - CH_API_KEY=""
    - CH_API_KEY_2=""
    - CH_API_KEY_3=""

You must have at least one API key, but keys 2 and three may be left as an empty string.

It is recommended that you set up a Python virtual environment for this project. You will need a recent version of Python to do this.

Navigate to your directory of choice, and clone this repository. 

Then run the following CLI command on a Unix-based system (Linux/Mac):

```bash
source venv/bin/activate
```

For Windows 
```shell
venv\Scripts\activate
```

To install dependencies, run: 
```bash
python install -r requirements.txt
```

You must have at least one use of land dataset from both UK companies and 
overseas companies located in ~/datasets directory.

Finally, it is recommended that you set up a new database schema to hold the
data generated by this tool.

To start the CLI tool, run python main.py from the root directory of the repository.

## Usage

### Extracting dataset data

The CLI depends upon data available held by a MySQL database. Therefore it's necessary to first collect some datasets, place them in the datasets directory, and run the HMLR dataset extraction command.

Once you have the datasets in place, run:

```bash
python main.py --hmlr both
```

The --hmlr flag indicates that we want to extract data from a dataset and push it into the database.

The "both" argument refers to both overseas and UK company datasets. If you are only extracting UK company datasets, the use the "dom" argument, and for overseas company datasets, use "for", e.g.:

```bash
python main.py --hmlr dom
```

The command will generate three .txt files in the datasets directory, listing all uniquely named companies and their numbers, as well as a list of all files which have been parsed.

### Enriching with Companies House data

Once you have ingested some datasets, you can further enrich the data by making systematic queries to the Companies House API to obtain beneficial owner data and charge information.  

With regards charge information, the tool collects the parties entitled to the charged interests held by a company. Companies and individuals that appear in the charge owner information will not be matched with existing company names or beneficial owners in already appearing in the database - you will need to make targeted JOIN statements to achieve this. 

To obtain charge data, run the following command:

```bash
python main.py --ch charge
```

To obtain beneficial owner data for foreign companies, run the following command:

```bash
python main.py --ch for
```

To obtain beneficial owner data for UK companies, run the following command:

```bash
python main.py --ch uk
```