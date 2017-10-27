server sent event example
=========================

Prerequisite: have redis-server running

Steps:

1) package to <jar>

2) run microservice:

	java -jar <jar> [-Devent.cache.expire=10]
	Note: cache entry expiration in seconds. default 10s 

3) run emitter:

	one off - 
		curl -H "Content-Type: application/json" -X POST localhost:8080/events -d'{"a":1,"b":2, "c":"d", "e":["f1", "f2", "f3"], "f":{"g":1, "h":"h2"}}'
	
	or long running - 
		for i in {1..100000}; do \
			msg=PID:$$-TIME:`date +%x-%X|sed 's/[\/,:, ]/-/g'`; \
			sleep 1; \
			echo $msg; \ 
			curl -X POST localhost:8080/events/$msg; \ 
		done;
	
4) run listener:

	browser - <host>:<port>/sse.html
	
	or curl -H'accept: text/event-stream' <host>:<port>/events
	
	example optional query: //*[contains(local-name(),'Name')]	
