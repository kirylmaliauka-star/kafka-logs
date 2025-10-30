# -*- coding: utf-8 -*-
"""
This module provides a handler for processing Oracle logs and publishing them to Kafka.
It includes functions to parse configuration, decode secrets from OCI Vault,
append metadata, parse Oracle metadata, and publish messages to a Kafka topic.
It uses the OCI SDK for Python to interact with Oracle Cloud Infrastructure services
and the Confluent Kafka library to produce messages to Kafka.
"""


import io
import sys
import json
import logging
import uuid
import base64

import oci
from confluent_kafka import Producer
from confluent_kafka.error import KafkaException, KafkaError

logger = logging.getLogger(__name__)


def parse_config(ctx):
    """
    Parse the configuration for the function.
    :param Any ctx: context from the Oracle Function service.
    """

    def parse_kafka_config(config):
        """
        Parse the Kafka configuration from raw config.
        """
        kafka_config = json.loads(config.get("kafka_config", {}))
        if not kafka_config:
            logger.error("No Kafka configuration found in context")
            sys.exit()
        if "bootstrap.servers" not in kafka_config:
            logger.error("Missing 'bootstrap.servers' in Kafka configuration")
            sys.exit()
        return kafka_config

    try:
        raw_config = ctx.Config()
    except KeyError:
        logger.error("Context doesn't contain config")
        sys.exit()
    try:

        config = {
            "kafka_config": parse_kafka_config(raw_config),
            "topic_name": raw_config.get("topic_name"),
            "ca_cert_secret_name": raw_config.get("ca_cert_secret_name", ""),
            "client_cert_secret_name": raw_config.get("client_cert_secret_name", ""),
            "client_key_secret_name": raw_config.get("client_key_secret_name", ""),
            "vauld_ocid": raw_config.get("vauld_ocid"),
            "index": raw_config.get("index", "infra"),
            "field_name": raw_config.get("field_name", ""),
            "tags": raw_config.get("log_tags", ""),
            "log_level": raw_config.get("log_level", "info")
        }
    except KeyError as ex:
        logger.error("Missing configuration key: %s", str(ex))
        sys.exit()
    return config


def append_metadata(config, single, identity_client, logging_client):
    """
    Append static,config metadata to the log entry.
    :param dict config: configuration dictionary
    :param oci.signer.Signer signer: OCI signer for authentication
    :param oci.identity.IdentityClient identity_client: OCI Identity client
    :param oci.logging.LoggingManagementClient logging_client: OCI Logging client
    :return: single log entry with appended metadata
    """

    random_uuid = uuid.uuid4()

    uuid_bytes = random_uuid.bytes[:15]
    base64_encoded = base64.b64encode(uuid_bytes).decode('utf-8')

    filebeat_id = base64_encoded.rstrip('=')

    single["@metadata"] = {
            "_id": filebeat_id,
            "beat": "filebeat",
            "type": "_doc",
            "version": "8.17.0",
            "raw_index": config.get("index")
            }
    single["tags"] = str(config.get("tags")).split(",")
    single["@timestamp"] = single.get("time")
    if "oracle" in single:
        single["oracle_parsed"] = {}
        if "compartmentid" in single["oracle"]:
            try:
                single["oracle_parsed"]["compartment_name"] = identity_client.get_compartment(compartment_id=single["oracle"]["compartmentid"]).data.name
            except Exception as ex:
                logger.error("Can't get compartment by id: %s", ex)
            finally:
                single["oracle_parsed"]["compartment_id"] = "not available"

        if "loggroupid" in single["oracle"]:
            if str(single["oracle"]["loggroupid"]).startswith("ocid1"):
                try:
                    single["oracle_parsed"]["log_group_name"] = logging_client.get_log_group(log_group_id=single["oracle"]["loggroupid"]).data.display_name
                except Exception as ex:
                    logger.error("Can't get log group by id: %s", ex)
                finally:
                    single["oracle_parsed"]["log_group_id"] = "not available"
        if "ingestedtime" in single["oracle"]:
            single["oracle_parsed"]["ingested_time"] = single["oracle"]["ingestedtime"]

    return single


def decode_secret(secret_client, config, secret_name=None):
    """
    Decode a secret from OCI Vault using the provided signer and configuration.
    :param oci.secrets.SecretsClient secret_client: OCI Secrets client for get secret bundle
    :param dict config: configuration dictionary containing vault information
    :param str secret_name: name of the secret to decode
    :return: decoded secret value
    """

    try:
        secret = secret_client.get_secret_bundle_by_name(
            secret_name=secret_name,
            vault_id=config["vauld_ocid"])
    except Exception as ex:
        logger.error("Error retrieving bundle by name: %s", ex)
        sys.exit(1)

    secret_content = secret.data.secret_bundle_content.content.encode("utf-8")

    return base64.b64decode(secret_content).decode("utf-8")


def publish_message(config, producer, data):
    """
    Publish a message to the Kafka topic using the provided configuration and certificate.
    :param dict config: configuration dictionary containing Kafka settings
    :param confluent_kafka.Producer producer: Kafka producer instance
    :param list data: list of data to be sent to Kafka
    :return: number of messages sent
    """

    def acked(err, msg=None):
        if err is not None:
            logger.error("Failed to deliver message: %s", str(err))
        elif msg is not None:
            logger.debug("Message produced to %s[%d] at offset %d", msg.topic(), msg.partition(), msg.offset())
    try:
        count = 0
        for i, single in enumerate(data):
            message_value = json.dumps(single).encode('utf-8')

            producer.produce(config["topic_name"], key=None,  value=message_value, callback=acked)
            count += 1
            if i % 256 == 0:
                logger.info("Produced %d messages so far", i)
                producer.poll(1)

        producer.flush()
    except ConnectionRefusedError as e:
        logger.error("Connection refused: %s", e)
    except KafkaException as e:
        logger.error("Kafka exception occurred: %s", e)
    except KafkaError as e:
        logger.error("Kafka error occurred: %s", e)
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)
    finally:
        producer.flush()

    return count


def handler(ctx, data: io.BytesIO = None):
    """
    Main handler function that inveoked by the Oracle Function service.
    :param Any ctx: context from the Oracle Function service
    :param io.BytesIO data: input data, expected to be in JSON format
    """

    config = parse_config(ctx)
    logger.info("All config done")

    try:
        log_level = config.get("log_level").upper()
        logger.setLevel(getattr(logging, log_level))
        logging.getLogger().setLevel(getattr(logging, log_level))  # Set the root logger level for urllib3 and other libraries
    except AttributeError:
        logger.error("Invalid log level: %s. Valid levels: DEBUG, INFO, WARNING, ERROR, CRITICAL", config.get("log_level"))
        logger.setLevel(logging.ERROR)

    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        identity_client = oci.identity.IdentityClient(config={}, signer=signer)
        logging_client = oci.logging.LoggingManagementClient(config={}, signer=signer)
        secret_client = oci.secrets.SecretsClient(config={}, signer=signer)
    except Exception as ex:
        logger.error("Error creating OCI clients: %s", ex)
        sys.exit(1)

    try:
        body = json.loads(data.getvalue())
    except (Exception, ValueError) as ex:
        logger.error("Error parsing the json data: %s", ex)
        sys.exit(1)

    logger.info("All data parsed")
    kafka_config = config.get("kafka_config")
    if kafka_config.get("security.protocol", "") == "SSL":
        try:
            kafka_config['ssl.ca.pem'] = decode_secret(secret_client, config, config.get("ca_cert_secret_name"))
            kafka_config['ssl.certificate.pem'] = decode_secret(secret_client, config, config.get("client_cert_secret_name"))
            kafka_config['ssl.key.pem'] = decode_secret(secret_client, config, config.get("client_key_secret_name"))
            config['kafka_config'] = kafka_config

        except (Exception, ValueError) as ex:
            logger.error("Error retrieving the client certificate: %s", ex)
            sys.exit(1)
    logger.info("All certificates decoded")

    try:
        producer = Producer(config['kafka_config'])
    except Exception as ex:
        logger.error("Producer creation failed: %s", ex)
        sys.exit(1)
    logger.info("Producer created")

    if not isinstance(body, list):
        body = [body]
        logger.info("Data is not a list, converting to list")
    for i, single in enumerate(body):
        if "oracle" in single:
            body[i] = append_metadata(
                config=config,
                single=single,
                identity_client=identity_client,
                logging_client=logging_client
                )

    sent_count = publish_message(config=config, producer=producer, data=body)
    logger.info("Sent %d messages to Kafka topic %s", sent_count, config['topic_name'])
