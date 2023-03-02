import serial
import time
import argparse
import parse
import logging
import sched

from datetime import datetime
from waggle.plugin import Plugin, get_timestamp

def parse_values(sample, **kwargs):
    # Note: Specific to WXT ASCII query commands
    if '0R0' in sample:
        parms = ['Dm', 'Sm', 'Ta', 'Ua', 'Pa', 'Rc', 'Th', 'Vh']
        data = parse.search("Dm={3D}D," +
                            "Sm={.1F}M," +
                            "Ta={.1F}C," +
                            "Ua={.1F}P," +
                            "Pa={.1F}H," +
                            "Rc={.2F}M," +
                            "Th={.1F}C," +
                            "Vh={.1F}#" ,
                            sample
                           )
        ndict = dict(zip(parms, data))
                 
    return ndict

def set_mode(dev: serial.Serial, mode: str):
    """
    Send the `mode` setting and attempt to verify (echo of `mode`).
    Exception on failure.
    """
    for i in range(5):
        logging.debug(f"writing command[try: {i}] '{mode}'")
        dev.write("{}\n".format(mode).encode())
        if validate_response(dev, lambda x: x == mode.lower()):
            return
    raise Exception(f"Unable to validate command '{mode}'")

def request_sample(dev: serial.Serial) -> str:
    """
    Poll the WXT530 and read the return data
    """
    while True:
        # initalize communciation with the instrument
        logging.debug("send command to instrument to start poll")
        dev.write(b'0R0\r\n')
        line = validate_response(dev, lambda x: x.startswith("0R0"))
        if line:
            return line
        
def validate_response(dev: serial.Serial, test) -> str:
    """
    Validate the response is not empty and matches `test` criteria.
    Returns response on success, `None` on failure.
    """
    # try for up-to 3 seconds to get a response
    wait = 0.1
    loops = 30
    for i in range(loops):
        data = dev.readline()
        logging.debug(f"read data [{data}]")
        if data == b"":
            time.sleep(wait)
            break
        try:
            line = data.decode().strip()
        except UnicodeDecodeError:
            time.sleep(wait)
            continue
        if not test(line):
            time.sleep(wait)
            continue
        # all tests passed, return response
        return line
    return None

def sample_and_publish_task(scope, dev, plugin, delay, publish_names):
    # Update the log
    logging.info("requesting sample for scope %s", scope)
    # Define the timestamp
    timestamp = get_timestamp()
    # Request Sample
    sample = request_sample(dev)
    print('Input: ', sample)
    # Parse the readline
    values = parse_values(sample)
    # Update the log
    logging.debug("read values %s", values)

    # publish each value in sample
    for name, key in publish_names.items():
        try:
            value = values[key]
        except KeyError:
            continue
        # Update the log
        logging.info("publishing %s %s", name, value)
        plugin.publish(name, value=value, scope=scope, timestamp=timestamp)

def start_publishing(args, plugin, dev, **kwargs):
    """
    start_publishing initializes the Visala WXT530
    Begins sampling and publishing data

    Functions
    ---------


    Modules
    -------
    plugin
    logging
    sched
    parse
    """
    # initialize the raingauge
    ##try:
    ##    logging.info("set to polling mode")
    ##    set_mode(dev, "p")
    ##except:
    ##    pass
    ##try:
    ##   logging.info("set to high precision")
    ##    set_mode(dev, "h")
    ##except:
    ##    pass
    ##try:
    ##    logging.info("set to metric mode")
    ##    set_mode(dev, "m")
    ##except:
    ##    pass

    sch = sched.scheduler(time.time, time.sleep)

    # setup and run publishing schedule
    if args.node_publish_interval > 0:
        ##sample_and_publish_task("node", args.node_publish_interval)
        sch.enter(0, 
                  0, 
                  sample_and_publish_task, 
                  kwargs={"scope" : "node",
                          "dev" : dev,
                          "plugin" : plugin,
                          "delay" : args.node_publish_interval,
                          "publish_names" : kwargs['names'],
                        }
                )

    ##if args.beehive_publish_interval > 0:
    ##    sch.enter(0, 
    ##              0, 
    ##              sample_and_publish_task, 
    ##              kwargs={"scope": "beehive",
    ##                      "delay": args.beehive_publish_interval,
    ##                    }
    ##            )

    sch.run()

def main():
    publish_names = {"winddir": "Dm",
                     "windspd": "Sm",
                     "airtemp": "Ta",
                     "relhumid": "Ua",
                     "pressure": "Pa",
                     "rainaccum": "Rc",
                     "heattemp": "Th",
                     "heatvolt": "Vh"
                    }

    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", 
                        action="store_true", 
                        help="enable debug logs"
                        )
    parser.add_argument("--device", 
                        default="/dev/ttyUSB0", 
                        help="serial device to use"
                        )
    parser.add_argument("--baudrate", 
                        default=19200, 
                        type=int, 
                        help="baudrate to use"
                        )
    parser.add_argument("--node-publish-interval", 
                        default=0.2, 
                        type=float, 
                        help="interval to publish data to node " +
                             "(negative values disable node publishing)"
                        )
    parser.add_argument("--beehive-publish-interval", 
                        default=-1, 
                        type=float, 
                        help="interval to publish data to beehive " +
                             "(negative values disable beehive publishing)"
                        )
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(message)s",
                         datefmt="%Y/%m/%d %H:%M:%S",
                        )
    
    with Plugin() as plugin, serial.Serial(args.device, baudrate=args.baudrate, timeout=1.0) as dev:
        print(dev)
        start_publishing(args, plugin, dev, names=publish_names)

if __name__ == '__main__':
    main()
