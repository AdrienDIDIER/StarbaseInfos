# Image Airflow + deps systèmes + ton code + DAGs
FROM apache/airflow:2.9.3-python3.11

USER root
ENV DEBIAN_FRONTEND=noninteractive TZ=Europe/Paris
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr poppler-utils ffmpeg libgl1 tzdata \
 && rm -rf /var/lib/apt/lists/*


# Code applicatif
WORKDIR /opt/app
COPY requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && pip install -r /opt/app/requirements.txt

# Copie tout le projet (tes *.py, etc.)
COPY . /opt/app

# DAGs Airflow (wrappers) – voir dossier airflow_dags/ ci-dessous
COPY airflow_dags/ /opt/app/dags/

# Airflow lira ses DAGs ici
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/app/dags
# Pour que tes DAGs puissent "importer" ton code si besoin
ENV PYTHONPATH=/opt/app

# Rendre le port web visible
EXPOSE 8080
USER airflow
