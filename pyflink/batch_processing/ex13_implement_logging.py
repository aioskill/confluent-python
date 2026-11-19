import logging
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction

"""
To log metadata or sampled events directly from user-defined functions (UDFs), 
initialize the Python logging module inside your ProcessFunction or MapFunction.

## Where the Logs Go
Python logging writes to standard error/out inside the TaskManager container. 
Flink captures these logs via its SLF4J / Log4j2 framework:

Standalone / K8s: View aggregated logs using your log shipper (e.g., FluentBit, Vector, or Filebeat) 
collecting from /opt/flink/log/flink-*-taskexecutor-*.log.

Cloud (AWS Kinesis Analytics / GCP Cloud Dataflow): 
Logs automatically forward to CloudWatch or Cloud Logging.

"""
class LoggingMapFunction(MapFunction):

    def open(self, runtime_context):
        """
        Use open() for Logger Initialization: Always instantiate loggers in the open()
        lifecycle method of your UDF to prevent serialization errors when Flink
        ships the Python function to remote TaskManager processes.
        """
        # Configure logger per worker process execution context
        self.logger = logging.getLogger("PyFlinkJobLogger")
        self.logger.setLevel(logging.INFO)
        self.logger.info("Initializing MapFunction on TaskManager subtask: %s",
                         runtime_context.get_index_of_this_subtask())


    def map(self, value):
        # Log business events with structured metrics/context
        user_id, amount = value[0], value[1]

        if amount > 1000:
            self.logger.warning("High-value transaction detected: user=%s, amount=%.2f", user_id, amount)
        else:
            self.logger.debug("Processing order: user=%s, amount=%.2f", user_id, amount)
        return value


env = StreamExecutionEnvironment.get_execution_environment()

ds = env.from_collection(
    [("user_1", 1500.0), ("user_2", 45.0)],
    type_info=Types.TUPLE([Types.STRING(), Types.FLOAT()])
)
ds.print()
ds.map(LoggingMapFunction()).print()  # Process stream

env.execute("Logging example")

print("Job is completed")

