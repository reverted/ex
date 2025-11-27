
test:
	#docker run --name mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=true -p 3306:3306 -d mysql:8.0
	MYSQL_CONNECTION="root@tcp(localhost:3306)/" go test ./...
