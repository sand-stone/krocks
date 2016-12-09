rm -rf nohup*; nohup java -server -mx10g -cp ../target/kdb-1.0-SNAPSHOT.jar:. XProcess7 http://localhost:8000/ http://localhost:8001/ http://localhost:8002/ &
