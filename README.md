# grafana_middleware
Python middleware for PostgreSQL and InfluxDB

InfluxDB backup and restore

The Influx server must be running (influxd command). The following command must be 
executed on the host machine

influxd backup --portable -db NAME_OF_THE_DB PATH_TO_SAVE_BACKUP

To restore a database from a backup directory created by the backup command, the Influx server must be running (influxd command).  The following command must be executed on the host machine

influxd restore -portable PATH_TO_BACKUP_DIRECTORYss
