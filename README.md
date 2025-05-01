# multimodal-stock-prediction
conda env create -f environment.yml


# clone / replace files, then:
conda env update -f environment.yml --prune
conda activate sentiment-stocks


# launch everything locally
bash run_pipeline.sh
