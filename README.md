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

## Pre-requisites 

You will need at least one Companies House API key, although it is recommended to 
use no more than three keys to maximise output.

The CLI requires a .env file located at the root of the project with the following
variables:
    MYSQL_HOST=""
    MYSQL_USER=""
    MYSQL_PASSWORD=""
    MYSQL_D=""
    CH_API_KEY=""
    CH_API_KEY_2=""
    CH_API_KEY_3=""

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
