# multimodal-stock-prediction
conda env create -f environment.yml


# clone / replace files, then:
conda env update -f environment.yml --prune
conda activate sentiment-stocks

# spin up infra (sample docker-compose.yml in repo root)
docker compose up -d kafka zookeeper elasticsearch

# launch everything locally
bash run_pipeline.sh
