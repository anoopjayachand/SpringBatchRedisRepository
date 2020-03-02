package com.leo.app.config;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

@Configuration
public class RedisConfig {
	private RedisServer redisServer;

	@Bean
	RedisConnectionFactory jedisConnectionFactory() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("localhost", 6379);
		return new JedisConnectionFactory(config);
	}

	@Bean
	RedisTemplate<?, ?> redisTemplate() {
		/*
		 * ObjectMapper om = new ObjectMapper(); om.setVisibility(PropertyAccessor.ALL,
		 * JsonAutoDetect.Visibility.ANY);
		 */

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
