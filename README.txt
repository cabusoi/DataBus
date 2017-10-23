server sent event example
=========================

1) package to <jar>

2) run microservice:

	java -jar <jar>

3) run emitter:

	one off - 
		curl -H "Content-Type: application/json" -X POST localhost:8080/events -d'{"a":1,"b":2, "c":"d", "e":["f1", "f2", "f3"], "f":{"g":1, "h":"h2"}}'
	
	or long running - 
		for i in {1..100000}; do \
			msg=PID:$$-TIME:`date +%s`;
			sleep 1;
			echo $msg; 
			curl -X POST localhost:8080/events/$msg; 
		done;
	
4) run listener:

	browser - <host>:<port>/sse.html
	
	or curl -H'accept: text/event-stream' <host>:<port>/events	