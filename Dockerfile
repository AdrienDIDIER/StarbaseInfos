# Image Airflow + deps système + ton code + DAGs
FROM apache/airflow:2.9.3-python3.11

# --- root pour les paquets système ---
USER root
ENV DEBIAN_FRONTEND=noninteractive TZ=Europe/Paris
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr poppler-utils ffmpeg libgl1 tzdata \
 && rm -rf /var/lib/apt/lists/*

# Préparer le dossier app et donner les droits à 'airflow'
RUN mkdir -p /opt/app && chown -R airflow:root /opt/app
WORKDIR /opt/app

# --- repasser en 'airflow' avant pip install ---
USER airflow
# (si ton requirements.txt est à la racine du repo)
COPY --chown=airflow:root requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/app/requirements.txt

# Code + DAGs (toujours en 'airflow')
COPY --chown=airflow:root . /opt/app
# Si tes DAGs sont dans ./airflow_dags/, on les place là où Airflow les lit :
# (si tu as déjà ce dossier dans /opt/app/dags, garde-le)
COPY --chown=airflow:root airflow_dags/ /opt/app/dags/

# Airflow lira ses DAGs ici, et tes scripts peuvent être importés
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/app/dags
ENV PYTHONPATH=/opt/app

EXPOSE 8080
# rester en 'airflow' pour l'exécution
