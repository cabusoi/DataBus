package com.booxware.retail.service.bus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.jxpath.JXPathContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.fasterxml.jackson.databind.JsonNode;

/*
 * server sent event example
=========================

1) package to <jar>

2) run microservice:

	java -jar <jar>
	
3) execute redis-server	

4) run emitter:

	one off - 
		curl -H "Content-Type: application/json" -X POST localhost:8080/events -d'{"a":1,"b":2, "c":"d", "e":["f1", "f2", "f3"], "f":{"g":1, "h":"h2"}}'
	
	or long running - 
		for i in {1..100000}; do \
			msg=PID:$$-TIME:`date +%s`;
			sleep 1;
			echo $msg; 
			curl -X POST localhost:8080/events/$msg; 
		done;
	
5) run listener:

	browser - <host>:<port>/sse.html
	
	or curl -H'accept: text/event-stream' <host>:<port>/events	
	
 * */
@SpringBootApplication
@RestController
@EnableCaching
public class ProgramDataBusApplication {

	@Value("${http.connection.timeout}")
	private int httpTimeout;

	@Value("${event.cache.expire}")
	private int eventTTL;
	
	public static void main(String[] args) {
		SpringApplication.run(ProgramDataBusApplication.class, args);
	}

	@Bean
	AsyncSupportConfigurer configurer() {
		AsyncSupportConfigurer configurer = new AsyncSupportConfigurer();
		configurer.setDefaultTimeout(httpTimeout);
		return configurer;
	}

	Map<Object, Object> eventsCache;
	Map<SseEmitter, String> subscribersAndFilters;

	public ProgramDataBusApplication(StringRedisTemplate template) {
		DefaultRedisMap<?,?> map = new DefaultRedisMap<Object, Object>("events", template);
		this.eventsCache = (Map<Object, Object>) map;
		this.subscribersAndFilters = new HashMap<>();
	}

	@PostConstruct
	public void setup(){
		((DefaultRedisMap<?,?>)this.eventsCache).expire(eventTTL, TimeUnit.SECONDS);
	}
	
	@PostMapping(path = "/events/{event}")
	public void publish(@PathVariable("event") String event) throws IOException {
		sendToSubscribers(event);
	}

	@PostMapping(path = "/events", consumes = { "application/x-www-form-urlencoded;charset=UTF-8", "application/json" })
	public void publish(@RequestBody JsonNode event) throws IOException {
		sendToSubscribers(event);
	}

	@GetMapping(path = "/events", produces = "text/event-stream")
	public SseEmitter subscribe(@RequestParam(name = "query", defaultValue = "") String xpath) {

		SseEmitter emitter = new SseEmitter();
		filterAndSend(emitter, eventsCache.keySet(), xpath);

		subscribersAndFilters.put(emitter, xpath);
		Runnable remove = () -> {
			subscribersAndFilters.remove(emitter);
		};
		emitter.onCompletion(remove);
		emitter.onTimeout(remove);
		return emitter;
	}

	private void sendToSubscribers(Object event) {
		eventsCache.put(event, event);

		List<?> eventList = Arrays.asList(event);
		for (Entry<SseEmitter, String> entry : subscribersAndFilters.entrySet()) {
			SseEmitter emitter = entry.getKey();
			String filter = entry.getValue();
			try {
				filterAndSend(emitter, eventList, filter);
			} catch (IllegalStateException ex) {
				emitter.completeWithError(ex);
			}
		}
	}

	private void filterAndSend(SseEmitter emitter, Collection<?> events, String xpath) {
		Iterator<?> iterator;
		if (xpath.isEmpty()) {
			iterator = events.iterator();
		} else {
			JXPathContext context = JXPathContext.newContext(events);
			iterator = context.iterate(xpath);
		}
		iterator.forEachRemaining(event -> {
			try {
				emitter.send(SseEmitter.event().data(event));
			} catch (IOException e) {
			}
		});
	}

}
