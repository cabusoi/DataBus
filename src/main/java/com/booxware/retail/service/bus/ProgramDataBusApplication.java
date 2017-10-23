package com.booxware.retail.service.bus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.jxpath.JXPathContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheBuilderSpec;


/*
 * server sent event example
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
	
 * */
@SpringBootApplication
@RestController
@EnableCaching
public class ProgramDataBusApplication {

	private static final int CACHE_CAPACITY = 100;
	private static final int CACHE_TIMEOUT = 10000;
	private static final int HTTP_CONNECTION_TIMEOUT = 1000000;

	public static void main(String[] args) {
		SpringApplication.run(ProgramDataBusApplication.class, args);
	}

	@Bean
	@Profile("guava")
	CacheBuilderSpec cacheBuilderSpec() {
		return CacheBuilderSpec.parse("maximumSize=100,expireAfterAccess=1000");
	}

	Cache events; 
	List<String> activeEvents;
	
	Map<SseEmitter,String> subscribersWithFilters;

	public ProgramDataBusApplication(CacheManager cacheManager) {
		this.events=cacheManager.getCache("events");
		this.activeEvents=new ArrayList<>();
		this.subscribersWithFilters = new HashMap<>();
	}
	
	@GetMapping(path = "/events", produces = "text/event-stream")
	public SseEmitter subscribe(@RequestParam(name="query",defaultValue="")String xpath) {
		
		SseEmitter emitter = new SseEmitter();
		sendEventsFiltered(emitter, activeEvents, xpath);
		
		subscribersWithFilters.put(emitter,xpath);
		Runnable remove=() -> {subscribersWithFilters.remove(emitter);};
		emitter.onCompletion(remove);
		emitter.onTimeout(remove);
		return emitter;
	}

	private void sendEventsFiltered(SseEmitter emitter, Collection<String> events, String xpath) {
		Iterator<?> iterator;
		if(xpath.isEmpty()){
			iterator=events.iterator();
		}else{
			JXPathContext context=JXPathContext.newContext(events);
			iterator=context.iterate(xpath);
		}
		iterator.forEachRemaining(event->{
			try {
				emitter.send(SseEmitter.event().data(event));
			} catch (IOException e) {
				activeEvents.remove(event.toString());
			}
		});
	}

	@PostMapping(path = "/events/{event}")
	public void publish(@PathVariable("event") String event) throws IOException {
		sendToSubscribers(event);
	}

	@PostMapping(path = "/events", consumes={"application/x-www-form-urlencoded;charset=UTF-8", "application/json"})
	public void publish(@RequestBody JsonNode event) throws IOException {
		sendToSubscribers(event);
	}

	private void sendToSubscribers(Object event) {
		events.put(event.toString(), event);
		activeEvents.add(event.toString());

		List<String> eventList = Arrays.asList(event.toString());
		for (Entry<SseEmitter,String> entry : subscribersWithFilters.entrySet()) {
			SseEmitter emitter=entry.getKey();
			String xPathFilter=entry.getValue();
			try {
				sendEventsFiltered(emitter, eventList, xPathFilter);
			} catch (IllegalStateException ex) {
				emitter.completeWithError(ex);
			}
		}
	}
}
