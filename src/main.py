from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.common import WatermarkStrategy, Encoder, Row
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetResetStrategy
from pyflink.datastream.state import ValueStateDescriptor
from dotenv import load_dotenv
import os

load_dotenv()

kafka_host = os.getenv('KAFKA_HOST')

class CountWindowAverage(FlatMapFunction):
    def __init__(self):
        self.sum = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "average",
            Types.TUPLE([Types.INT(), Types.INT()])
        )
        self.sum = runtime_context.get_state(descriptor)

    def flat_map(self, value):
        current_sum = self.sum.value()
        if current_sum is None:
            current_sum = (0, 0)

        current_sum = (current_sum[0] + 1, current_sum[1] + value[3])

        self.sum.update(current_sum)

        if current_sum[0] >= 2:
            self.sum.clear()
            yield Row(value[0], current_sum[0], current_sum[1])
            # yield {"value": value[0], "sum": current_sum[0], "avg": current_sum[1]}

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(interval=3000)
env.set_python_executable("/home/mhtuan/anaconda3/envs/flink-env/bin/python")
# env.add_jars("file:////home/mhtuan/work/flink_kafka/flink-sql-connector-kafka-3.2.0-1.19.jar")
# env.add_classpaths("/home/mhtuan/work/flink_kafka/flink-sql-connector-kafka-3.2.0-1.19.jar", "/home/mhtuan/work/flink_kafka/flink-connector-kafka-3.2.0-1.19.jar")

print("create source")

type_info = Types.ROW_NAMED(["id", "hour_key", "name", "age"], [Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
type_info_flatmap = Types.ROW_NAMED(["value", "sum", "avg"], [Types.INT(), Types.INT(), Types.INT()])
deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=type_info).build()

source = KafkaSource.builder() \
    .set_bootstrap_servers(f"{kafka_host}:9091,{kafka_host}:9092,{kafka_host}:9093") \
    .set_topics("test-flink-3") \
    .set_group_id("flink-1") \
    .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
    .set_value_only_deserializer(deserialization_schema) \
    .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
ds.print()

ds = ds.key_by(lambda record: record["id"], key_type=Types.INT()).flat_map(CountWindowAverage(), output_type=type_info_flatmap)

sink = KafkaSink.builder() \
    .set_bootstrap_servers(f"{kafka_host}:9091,{kafka_host}:9092,{kafka_host}:9093") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("sink-topic-count")
            .set_value_serialization_schema(JsonRowSerializationSchema.builder().with_type_info(type_info_flatmap).build())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()
ds.sink_to(sink)

env.execute("kafka_sink_example")