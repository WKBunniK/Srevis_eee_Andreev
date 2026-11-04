FROM redis:7.4-alpine
COPY start-redis.sh start-sentinel.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-redis.sh /usr/local/bin/start-sentinel.sh
USER redis
ENTRYPOINT ["/usr/local/bin/start-redis.sh"]
