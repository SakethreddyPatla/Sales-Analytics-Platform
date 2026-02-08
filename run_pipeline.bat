@echo off
echo ====================================
echo SALES ANALYTICS PIPELINE - DOCKER
echo ====================================

echo Building Docker images..
docker-compose build 

echo Running data extraction...
docker-compose run --rm extract python extract_api_data.py 
docker-compose run --rm extract python generate_customer_data.py

echo Loading data to database...
docker-compose run --rm load python load_to_db.py

echo Verifying database...
docker-compose run --rm load python verify_database.py

echo ========================================
echo PIPELINE COMPLETE
echo ========================================
pause