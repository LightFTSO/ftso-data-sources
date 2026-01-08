FROM redis/redis-stack-server:latest AS module_source
FROM valkey/valkey:8

WORKDIR /data
RUN mkdir -p /usr/lib/valkey/modules

# Copy TimeSeries module
COPY --from=module_source /opt/redis-stack/lib/redistimeseries.so /usr/lib/valkey/modules/timeseries.so

CMD ["valkey-server", "--loadmodule", "/usr/lib/valkey/modules/timeseries.so"]