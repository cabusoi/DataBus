package com.booxware.retail.service.bus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;

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

	final ObjectMapper mapper = new ObjectMapper();
	Map<String, String> eventsCache;
	Map<SseEmitter, String> subscribersAndFilters;

	public ProgramDataBusApplication(StringRedisTemplate template) {
		this.eventsCache = new DefaultRedisMap<>("events", template);
		this.subscribersAndFilters = new ConcurrentHashMap<>();
	}

	@PostConstruct
	public void setup() {
		((DefaultRedisMap<?, ?>) this.eventsCache).expire(eventTTL, TimeUnit.SECONDS);
	}

	@PostMapping(path = "/events/{event}")
	public void publish(@PathVariable("event") String event) throws IOException {
		sendToSubscribers(new TextNode(event));
	}

	@PostMapping(path = "/events", consumes = { "application/x-www-form-urlencoded;charset=UTF-8", "application/json" })
	public void publish(@RequestBody JsonNode event) throws IOException {
		sendToSubscribers(event);
	}

	@GetMapping(path = "/events", produces = "text/event-stream")
	public SseEmitter subscribe(@RequestParam(name = "query", defaultValue = "") String xpath) throws IOException {

		SseEmitter emitter = new SseEmitter();
		filterAndSend(emitter, rebuidEvents(eventsCache.values()), xpath);

		subscribersAndFilters.put(emitter, xpath);
		Runnable remove = () -> {
			subscribersAndFilters.remove(emitter);
		};
		emitter.onCompletion(remove);
		emitter.onTimeout(remove);
		return emitter;
	}

	private Collection<JsonNode> rebuidEvents(Collection<String> values)  {
		Collection<JsonNode> result = new ArrayList<>();
		values.forEach((c) -> {
				try {
					result.add(mapper.readTree(c));
				} catch (IOException e) {
					e.printStackTrace();
				}
		});
		return result;
	}

	private void sendToSubscribers(JsonNode event) throws JsonProcessingException {
		eventsCache.put(String.valueOf(event.hashCode()), mapper.writeValueAsString(event));

		List<JsonNode> eventList = Arrays.asList(event);
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

	private void filterAndSend(SseEmitter emitter, Collection<JsonNode> events, String xpath) {
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
