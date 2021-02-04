import sys
from argparse import ArgumentParser
import s3fs
import fastparquet as fp
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient


def parse_args():
    arg_parser = ArgumentParser()

    arg_parser.add_argument("--path", required=True, help="S3 parquet files path")
    arg_parser.add_argument("--topic", required=True, help="Topic name")
    arg_parser.add_argument("--brokers", required=True, help="Kafka bootstrap server address")
    arg_parser.add_argument("--schema-registry", required=True, help="Schema Registry url")

    return arg_parser.parse_args()


def get_schema(registry, topic):
    sr = CachedSchemaRegistryClient({
        'url': registry
    })
    return sr.get_latest_schema(f'{topic}-value')[1]


def get_producer(brokers, registry, topic):
    schema = get_schema(registry, topic)
    return AvroProducer({
        'bootstrap.servers': brokers,
        'schema.registry.url': registry,
        'schema.registry.auto.register.schemas': False
    }, default_value_schema=schema)


def main():
    args = parse_args()

    s3 = s3fs.S3FileSystem()
    fs = s3fs.core.S3FileSystem()
    producer = get_producer(args.brokers, args.schema_registry, args.topic)

    all_paths = fs.glob(path=args.path)

    for path in all_paths:
        print(f'processing path {path}')
        fp_obj = fp.ParquetFile(path, open_with=s3.open)
        df = fp_obj.to_pandas()

        columns = df.columns.values.tolist()
        for row in df.values.tolist():
            record = dict(zip(columns, row))
            producer.produce(topic=args.topic, value=record)
        producer.flush()


if __name__ == '__main__':
    main()
