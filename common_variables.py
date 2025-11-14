from airflow.models import Variable

# Variables are set in the Airflow UI under Admin -> Variables
PATH_TO_CODE = Variable.get("PATH_TO_CODE")
PATH_TO_DATASETTE_FILES = Variable.get("PATH_TO_DATASETTE_FILES")
COMMON_ENV_VARS = {
    "https_proxy": Variable.get("https_proxy"),
    "http_proxy": Variable.get("http_proxy"),
    "no_proxy": Variable.get("no_proxy"),
    "EMAIL_RECEIVERS": Variable.get("EMAIL_RECEIVERS"),
    "EMAIL_SERVER": Variable.get("EMAIL_SERVER"),
    "EMAIL": Variable.get("EMAIL"),
    "FTP_SERVER": Variable.get("FTP_SERVER"),
    "FTP_USER": Variable.get("FTP_USER"),
    "FTP_PASS": Variable.get("FTP_PASS"),
    "ODS_API_KEY": Variable.get("ODS_API_KEY"),
}
