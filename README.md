<p align="center">
  <img src="https://capsule-render.vercel.app/api?text=Shopify%20Data%20Pipeline&animation=fadeIn&type=waving&color=gradient&height=100"/>
</p>

# Overview

This project provides a complete end-to-end pipeline for Shopify data. It is made up of several parts that allow us to:

- Pull data from Shopify's API into a database (Postgres for production and DuckDB for local development and testing)
- Run a 5 minute automated cron job triggered by GitHub actions on our self-hosted runner
- Listen to Shopify customer deletion webhooks and anonymise records in our self-contained database for GDPR compliance

# How it works

<img width="567" height="568" alt="Shopify-to-postgres drawio (3)" src="https://github.com/user-attachments/assets/b3adc2e7-379b-490b-894c-c84552523335" />

The pipeline has 3 main components:

## 1. Data transfer

- A python script fetches data from Shopify using their API
- Data is stores in either:
	- PostgreSQL (production, hosted within a database cluster in digital ocean)
	- DuckDB (Local testing)
## 2. Scheduled Cron Job

- A GitHub Actions workflow runs the python script every 5 mins **
- Hosted on a self-hosted GitHub runner inside a Digital Ocean droplet, which is a linux server
- Uses cron syntax and configuration for the Shopify API -> DB inside a YAML file

** *While the workflow is configured to run every five minutes, the actual execution interval can vary due to GitHub Actions’ scheduling limitations for self-hosted runners. This behaviour is inherent to the platform and cannot be avoided within the current stack.*
## 3. Webhook triggered anonymisation

- A FastAPI server listens for a Shopify `customers/delete` event
- When triggered, a separate python script hashes personal information in the database relating to the customer that is passed via the webhook

# Setup

## Prerequisites

- Python 3.10 (or +)
- ngrok (for local webhook testing)
- GitHub self-hosted runner setup via GitHub settings
- GitHub self-hosted installation on Digital Ocean
- Shopify and Postgres database API credentials

## Local installation

```bash
# Clone Repo
git clone https://github.com/hughlawrenceecd/shopify_to_postgres

# cd into wherever it is stored on your machine
cd Documents/Github/shopify_to_postgres

# If you don't have uv installed already, install
pip install uv

# Create local environment
uv venv --python 3.10

source .venv/bin/activate

# Install DLT
uv pip install -U dlt

# install dependencies
pip install -r requirements.txt

```

The `requirements.txt` file contains both local development dependencies (DuckDB-related) and production dependencies (PostgreSQL-related).  
To avoid unnecessary package installations, comment out unused packages in both `requirements.txt` and the relevant Python imports.

### Relevant for local testing
`dlt[duckdb]>=0.5.1`

### Relevant for production
`dlt[postgres]>=0.5.1`
`psycopg2-binary==2.9.10`
`cron-descriptor==1.4.5`

### General dependencies
`pendulum>=2.1`
 `dlt==1.14.1`
 `pipdeptree==2.9.6`
 `fastapi>=0.110`
 `uvicorn[standard]>=0.29`

## Testing the webhook locally

To test the webhook locally, you’ll need to expose your FastAPI server to the internet. This is done using `ngrok` (run outside the virtual environment):
```bash

ngrok http 8000

```

You’ll see an output containing a `Forwarding` URL (HTTPS). This will be used to configure the webhook in Shopify.

```bash
Session Status                online                                            

Account                       Hugh (Plan: Free)                                 

Update                        update available (version 3.26.0, Ctrl-U to update

Version                       3.22.0                                            

Region                        Europe (eu)                                       

Latency                       32ms                                              

Web Interface                 http://127.0.0.1:4040                             

Forwarding                    https://b7af69e62e81.ngrok-free.app -> http://localhost:8000

Connections                   ttl     opn     rt1     rt5     p50     p90       

                              22      0       0.00    0.00    0.12    5.97      

HTTP Requests
```

In Shopify Admin

- Go to **Settings > Notifications > Webhooks**
- Create a new `customers/delete` webhook
- Set the webhook URL to the `ngrok` forwarding address

Then, using FastAPI we create an endpoint. Ensure this is done within the virtual environment.

```bash
uvicorn webhook_server:app --reload --port 8000
```
# Configuration

## Environment variables (and where to find them)

Within the directory you will need the following data in a `secrets.toml` file found in `webhook_test/.dlt/secrets.toml`. 

```yaml
[sources.shopify_dlt]
private_app_password = "" 
access_token = ""
api_key = ""
api_secret_key=""
partner_api_key=""

[destination.postgres.credentials]
database = ""
username = ""
password = ""
host = ""
port = ""
sslmode = "require"
```

#### `private_app_password` 

- Go to the store's admin dashboard
- Click **Settings > Apps and sales channels**
- Click **Develop apps**
- Create an app (if you haven’t already)
- Go to **API credentials**
- Copy the **Admin API access token** → this is `private_app_password`

#### `access_token`, `api_key`, `api_secret_key` & `partner_api_key`

- Go to Shopify Partner Dashboard
- Click __Apps__ > __Select Partner App__ > __API Credentials__

#### Everything under `[destination.postgres.credentials]`

- Open Digital Ocean dashboard
- Open Databases and select database you wish to send Shopify data to
- Under __overview__ you will see the relevant information for that database, e.g

<img width="649" height="436" alt="Screenshot 2025-08-11 at 21 43 30" src="https://github.com/user-attachments/assets/a8b13a1d-adc8-4c4a-9c45-6e89bf1e069d" />

#### YAML file configuration

The yaml file in the directory needs to pull certain envrionment variables from github to ensure a secure environment for the remote runner/actions workflow. In order for these to work you need to add the relevant data into github and reference it in the YAML file. Luckily a lot of these are identical to the `secrets.toml` file

```yaml
env:

DESTINATION__POSTGRES__CREDENTIALS__DATABASE: ${{secrets.DESTINATION__POSTGRES__CREDENTIALS__DATABASE}}

DESTINATION__POSTGRES__CREDENTIALS__USERNAME: ${{secrets.DESTINATION__POSTGRES__CREDENTIALS__USERNAME}}

DESTINATION__POSTGRES__CREDENTIALS__HOST: ${{secrets.DESTINATION__POSTGRES__CREDENTIALS__HOST}}

DESTINATION__POSTGRES__CREDENTIALS__PORT: ${{secrets.DESTINATION__POSTGRES__CREDENTIALS__PORT}}

SOURCES__SHOPIFY_DLT__PRIVATE_APP_PASSWORD: ${{ secrets.SOURCES__SHOPIFY_DLT__PRIVATE_APP_PASSWORD }}

DESTINATION__POSTGRES__CREDENTIALS__PASSWORD: ${{ secrets.DESTINATION__POSTGRES__CREDENTIALS__PASSWORD }}
```

These need to be added to Github too:
- Go to the repo in Github
- Under __Settings__ > __Secrets and variables__ > __Actions__
- Go to the Envrionment variables window and select __Manage environment secrets__ 
- __New environment__ and ensure the env variable names match the Github secrets variable names
- The Database, Username, Host, Port, Priavte App Password and Credentials Password can all be found in the same place as the secrets.toml file. With `CREDENTIALS_PASSWORD` being the password within Digital Ocean for the selected database.


These are needed for the cron job to correctly run when installed on a remote server, the naming of the variables isn't random but selected pruposefully as the github runner is able to determine which vairable to use based on the name. 


# Server installation

This is the installation for Digital Ocean Droplets, which is essentially a remote linux server. 

Within the server, we have to create a new user, assign root level access to them and then prevent password checking for when the commands run. The lack of password is needed for the runners to spin up virtual environments for each cron job. 

```bash
# Create User
 adduser newUser

# Add new user to sudo group
 usermod -aG sudo newUser

# Change user to new user
 su - newUser

# Witihin the new user
 sudo visudo
 newUser ALL=(ALL) NOPASSWD:ALL

```


Then we must create a GitHub runner via GitHub. 

- Go to GitHub and open the repository
- Got o __Settings__ > __Actions__ > __Runners__

Create new self-hosted runner

Follow the commands within the server, they should look something like this:


```bash

# Create a folder  
 mkdir actions-runner && cd actions-runner

# Download the latest runner package  
 curl -o actions-runner-linux-x64-2.327.1.tar.gz -L https://github.com/actions/runner/releases/download/v2.327.1/actions-runner-linux-x64-2.327.1.tar.gz

# Optional: Validate the hash  
 echo "d68ac1f500b747d1271d9e52661c408d56cffd226974f68b7dc813e30b9e0575 actions-runner-linux-x64-2.327.1.tar.gz" | shasum -a 256 -c

# Extract the installer  
tar xzf ./actions-runner-linux-x64-2.327.1.tar.gz
```

Then we can initialise the runner. 

```bash
# configure the runner
./config.sh --unattended --url https://github.com/your-org/your-repo --token YOUR_TOKEN_HERE --name ecd-runner --labels staging,shopify

# Install as a service and start
sudo ./svc.sh install
sudo ./svc.sh start

# To check the status (optional)
sudo systemctl status actions.runner.*

# Test connectivity from Digital Ocean server (optional)
nc -zv private-db-hostname.ondigitalocean.com 25060

# Manual run if necessary (optional)
./run.sh
```

# Troubleshooting

A number of issues kept cropping up when re-trying this installation, some common problems are as follows:

- Ensure that the server/droplet is a "Trusted Source" for the relevant database. This is done in Digital Ocean within the database settings
- Ensure the actions workflow isn't disabled in GitHub under __Actions__ > __Your workflow name__ and click on the enable workflow button
- When creating and configuring the runner within the droplet, ensure the commands you followed from github (similar to the example above) are the config settings for Linux, they default to Apple/Mac iOS
- Ensure the labels in the YAML file match the labels used in the configuration, so for example, the labels below should be present in the YAML file along with the `self-hosted` label.

```bash
./config.sh --unattended --url https://github.com/your-org/your-repo --token YOUR_TOKEN_HERE --name ecd-runner --labels staging,shopify

```

- After installation, if the runners do not start, try manually starting the runner with 

```bash 
./run.sh
```

- In the `shopify_dlt_pipeline.py` file, ensure that the `destination` is set to the correct place, whether thats locally (`duckdb`) or the server (`postgres`)
