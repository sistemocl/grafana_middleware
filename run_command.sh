sudo docker run --detach --restart unless-stopped --name grafana_middleware  -v $(pwd):/opt/grafana_middleware -w /opt/grafana_middleware grafana_middleware python main.py
