FROM 741386957827.dkr.ecr.ap-south-1.amazonaws.com/common-for-all-env:python-3.12-slim-new AS dev

WORKDIR /app

COPY ./requirements.txt ./requirements.txt
COPY ./docker/entrypoint-dev.sh ./entrypoint-dev.sh
COPY ./docker/entrypoint-prod.sh ./entrypoint-prod.sh

RUN pip install --no-cache-dir -r requirements.txt && \
    chmod +x ./entrypoint-dev.sh ./entrypoint-prod.sh


FROM 694468762768.dkr.ecr.ap-south-1.amazonaws.com/prod-common-for-all-service:python-3.12-slim-new AS prod

WORKDIR /app

COPY ./requirements.txt ./requirements.txt
COPY ./docker/entrypoint-dev.sh ./entrypoint-dev.sh
COPY ./docker/entrypoint-prod.sh ./entrypoint-prod.sh
COPY . ./

RUN pip install --no-cache-dir -r ./requirements.txt && \
    chmod +x ./entrypoint-dev.sh ./entrypoint-prod.sh
