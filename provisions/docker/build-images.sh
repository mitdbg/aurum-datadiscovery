echo "Building Docker images..."
echo "-------------------------"

echo "Building nginx..."
docker build -t datahuborg/nginx provisions/nginx/