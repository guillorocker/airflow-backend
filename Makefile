include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 240
	docker exec airflow airflow users create --username admin --password admin --role Admin --firstname Guillermo --lastname Lujan --email admin@email.com
	docker exec airflow airflow connections add 'olap' --conn-uri 'postgresql://root:root@olap-db:5432/olap'

down:
	docker-compose down