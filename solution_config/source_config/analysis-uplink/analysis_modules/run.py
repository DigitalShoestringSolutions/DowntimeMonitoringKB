"""Configure analysis module as a downtime monitoring sensor adaptor.

Compares sensor readings to thresholds and posts alerts to MQTT if the comparison result changes.

One analysis module instance per sensor-machine link.
Use the recipe to create multiple sensor adaptor module instances if you would like automatic downtime event creation on multiple machines.

"""

import logging
import datetime

# Internal module imports
from trigger.engine import TriggerEngine
import config_manager
import paho.mqtt.publish as pahopublish
import output.kinabase
import json
import time
import sys


# Parse command-line arguments and configure logging again based on those
args = config_manager.handle_args()
logging.basicConfig(level=args["log_level"])
logger = logging.getLogger(__name__)

# Load configuration from config files
config = config_manager.get_config(
    args.get("module_config_file"), args.get("user_config_file")
)

if config.get("module_enabled") == False:
    logger.info("Analysis module is disabled, sleeping for an hour before restarting")
    time.sleep(3600)
    sys.exit(0)

# Initialize the trigger engine with loaded configuration
trigger = TriggerEngine(config)

@trigger.mqtt.event("downtime/event/+/+")
async def handle_downtime_new_state(topic, payload, config={}):
    logger.info(f"Received downtime new state message: {topic} {payload}")
    await output.kinabase.update_record(
        config,
        fields={"running": "running"},
        collection_id="machines",
        kb_pk_field="downtimeId",
        data_pk_field="machine",
    )(payload)


# Start the trigger engine and its scheduler/event loops
trigger.start()
