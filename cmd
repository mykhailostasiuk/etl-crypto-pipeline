docker compose build
docker compose --profile pipeline build

docker compose up -d
docker compose --profile pipeline up pipeline_transform -d
docker compose --profile pipeline up pipeline_load -d
docker compose --profile pipeline up pipeline_extract -d

docker compose down
docker compose --profile pipeline down
