package com.leo.app.config;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

/**
 * 
 * The RedisConfig configures RedisConnectionFactory and RedisTemplate.
 * 
 * And this application using an embedded Redis server, so we need to start
 * during the context loading. 
 * 
 * @author anoop
 *
 */
@Configuration
public class RedisConfig {

	@Autowired
	Environment environment;

	private RedisServer redisServer;

	@Bean
	RedisConnectionFactory jedisConnectionFactory() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("localhost", 6379);
		return new JedisConnectionFactory(config);
	}

	@Bean
	RedisTemplate<?, ?> redisTemplate() {

		GenericJackson2JsonRedisSerializer genericJackson2JsonRedisSerializer = new GenericJackson2JsonRedisSerializer();

		RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(jedisConnectionFactory());

		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setHashKeySerializer(new StringRedisSerializer());

		redisTemplate.setHashValueSerializer(genericJackson2JsonRedisSerializer);
		redisTemplate.setValueSerializer(genericJackson2JsonRedisSerializer);
		redisTemplate.setEnableTransactionSupport(false);
		redisTemplate.afterPropertiesSet();

		return redisTemplate;
	}

	@PostConstruct
	public void init() {
		redisServer = new RedisServerBuilder().port(6379).setting("maxmemory 256M").build();
		redisServer.start();
	}

	@PreDestroy
	public void destroy() {
		redisServer.stop();
	}

}
