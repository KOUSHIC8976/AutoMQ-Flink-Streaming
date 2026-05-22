import os
os.environ['TZ'] = 'UTC'  
os.environ['_JAVA_OPTIONS'] = '-Duser.timezone=UTC'

import pathlib
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    lib_dir = os.path.abspath("lib")
    jar_urls = [pathlib.Path(os.path.join(lib_dir, jar)).as_uri() for jar in os.listdir(lib_dir) if jar.endswith(".jar")]
    
    t_env.get_config().set("pipeline.jars", ";".join(jar_urls))
    t_env.get_config().set("table.exec.source.idle-timeout", "2000 ms")
    t_env.get_config().set("table.local-time-zone", "UTC")
    
    print(" Flink Environment Booted. Connecting to AutoMQ...")

    source_ddl = """
        CREATE TABLE automq_telemetry_source (
            satellite_id STRING,
            temperature_c DOUBLE,
            ts_string STRING, -- Read timestamp as string first from JSON
            ts AS TO_TIMESTAMP(ts_string, 'yyyy-MM-dd HH:mm:ss.SSS'),
            WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'automq_demo_topic',
            'properties.bootstrap.servers' = 'localhost:9092', /* Points to AutoMQ */
            'properties.group.id' = 'automq_flink_consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json' 
        )
    """
    t_env.execute_sql(source_ddl)
    sink_ddl = """
        CREATE TABLE print_sink (
            satellite_id STRING,
            window_end TIMESTAMP(3),
            max_temperature DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """
    t_env.execute_sql(sink_ddl)
    query = """
        INSERT INTO print_sink
        SELECT 
            satellite_id,
            TUMBLE_END(ts, INTERVAL '10' SECOND) AS window_end,
            MAX(temperature_c) AS max_temperature
        FROM automq_telemetry_source
        GROUP BY 
            TUMBLE(ts, INTERVAL '10' SECOND), 
            satellite_id
    """
    
    print(" Executing Stateful Tumbling Window Aggregation... (Waiting for 10 seconds of data)")
    t_env.execute_sql(query).wait()

if __name__ == '__main__':
    main()