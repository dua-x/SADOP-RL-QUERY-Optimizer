FROM python:3.10

# Dossier de travail dans le conteneur
WORKDIR /app

# Copier requirements.txt d'abord (optimisation Docker)
COPY app/requirements.txt /app/requirements.txt

# Installer les dépendances Python
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copier le reste du code
COPY app/ /app/

# Éviter les buffers (logs en temps réel)
ENV PYTHONUNBUFFERED=1

# Commande par défaut (docker-compose la remplacera)
CMD ["bash"]