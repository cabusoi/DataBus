package com.booxware.retail.service.bus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.PostConstruct;

import org.apache.commons.jxpath.JXPathContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.DependsOn;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.fasterxml.jackson.databind.JsonNode;

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
@DependsOn("cacheManager")
public class ProgramDataBusApplication {

	private static final int CACHE_CAPACITY = 100;
	private static final int CACHE_TIMEOUT = 10000;
	private static final int HTTP_CONNECTION_TIMEOUT = 1000000;

	public static void main(String[] args) {
		SpringApplication.run(ProgramDataBusApplication.class, args);
	}

	// @Bean
	// @Profile("guava")
	// CacheBuilderSpec cacheBuilderSpec() {
	// return CacheBuilderSpec
	// .parse(String.format("maximumSize=?,expireAfterAccess=?", CACHE_CAPACITY,
	// CACHE_TIMEOUT));
	// }
	//
	@Autowired
	CacheManager cacheManager;

	ListableCache eventsCache;
	Map<SseEmitter, String> subscribersAndFilters;

	@PostConstruct
	public void setup() {
		this.eventsCache = ListableCache.build(cacheManager.getCache("events"));
		this.subscribersAndFilters = new HashMap<>();
	}

	@GetMapping(path = "/events", produces = "text/event-stream")
	public SseEmitter subscribe(@RequestParam(name = "query", defaultValue = "") String xpath) {

		SseEmitter emitter = new SseEmitter();
		filterAndSend(emitter, eventsCache.keys(), xpath);

		subscribersAndFilters.put(emitter, xpath);
		Runnable remove = () -> {
			subscribersAndFilters.remove(emitter);
		};
		emitter.onCompletion(remove);
		emitter.onTimeout(remove);
		return emitter;
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
				events.remove(event);
			}
		});
	}

	@PostMapping(path = "/events/{event}")
	public void publish(@PathVariable("event") String event) throws IOException {
		sendToSubscribers(event);
	}

	@PostMapping(path = "/events", consumes = { "application/x-www-form-urlencoded;charset=UTF-8", "application/json" })
	public void publish(@RequestBody JsonNode event) throws IOException {
		sendToSubscribers(event);
	}

	private void sendToSubscribers(Object event) {
		eventsCache.put(event.toString(), event);

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
}

class ListableCache implements Cache {
	private final Set<Object> keys;
	private final Cache cache;

	public static ListableCache build(Cache cache) {
		return new ListableCache(cache);
	}

	private ListableCache(Cache cache) {
		this.cache = cache;
		keys = new HashSet<>();
	}

	public Set<Object> keys() {
		return keys;
	}

	@Override
	public void clear() {
		cache.clear();
		keys.clear();
	}

	@Override
	public void evict(Object arg0) {
		cache.evict(arg0);
		keys.remove(arg0);
	}

	@Override
	public <T> T get(Object arg0, Callable<T> arg1) {
		keys.add(arg0);
		return cache.get(arg0, arg1);
	}

	@Override
	public <T> T get(Object arg0, Class<T> arg1) {
		keys.add(arg0);
		return cache.get(arg0, arg1);
	}

	@Override
	public ValueWrapper get(Object arg0) {
		keys.add(arg0);
		ValueWrapper result = get(arg0);
		if (result == null) {
			keys.remove(arg0);
		}
		return result;
	}

	@Override
	public void put(Object arg0, Object arg1) {
		keys.add(arg0);
		cache.put(arg0, arg1);
	}

	@Override
	public ValueWrapper putIfAbsent(Object arg0, Object arg1) {
		keys.add(arg0);
		return cache.putIfAbsent(arg0, arg1);
	}

	@Override
	public String getName() {
		return cache.getName();
	}

	@Override
	public Object getNativeCache() {
		return cache.getNativeCache();
	}

}
