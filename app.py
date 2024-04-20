import serial
import argparse
import parse
import logging
import datetime

from waggle.plugin import Plugin, get_timestamp

def parse_values(sample, **kwargs):
    # Note: Specific to WXT ASCII query commands
    if sample.startswith(b'0R0'):
        parms = ['Dm', 'Sm', 'Ta', 'Ua', 'Pa', 'Rc', 'Th', 'Vh']
        # Note: ASCII sting changes voltage heater character if 
        # voltage is supplied or not.
        #   - '#' for voltage not supplied
        #   - 'N' for supplied voltage and above heating temp
        #   - 'V' heating is on at 50% duty cycle, between high and middle control
        #   - 'W' heating is on 100% duty cycle, between low and middle control temps
        #   - 'F' heating is on at 50% duty cycle, heating temp below low control temp
        if sample.endswith(b'N'):
            data = parse.search("Dm={3D}D," +
                                "Sm={.1F}M," +
                                "Ta={.1F}C," +
                                "Ua={.1F}P," +
                                "Pa={.1F}H," +
                                "Rc={.2F}M," +
                                "Th={.1F}C," +
                                "Vh={.1F}N" ,
                                sample.decode('utf-8')
                               )
        elif sample.endswith(b'V'):
            data = parse.search("Dm={3D}D," +
                                "Sm={.1F}M," +
                                "Ta={.1F}C," +
                                "Ua={.1F}P," +
                                "Pa={.1F}H," +
                                "Rc={.2F}M," +
                                "Th={.1F}C," +
                                "Vh={.1F}V" ,
                                sample.decode('utf-8')
                               )
        elif sample.endswith(b'W'):
            data = parse.search("Dm={3D}D," +
                                "Sm={.1F}M," +
                                "Ta={.1F}C," +
                                "Ua={.1F}P," +
                                "Pa={.1F}H," +
                                "Rc={.2F}M," +
                                "Th={.1F}C," +
                                "Vh={.1F}W" ,
                                sample.decode('utf-8')
                               )
        elif sample.endswith(b'F'):
            data = parse.search("Dm={3D}D," +
                                "Sm={.1F}M," +
                                "Ta={.1F}C," +
                                "Ua={.1F}P," +
                                "Pa={.1F}H," +
                                "Rc={.2F}M," +
                                "Th={.1F}C," +
                                "Vh={.1F}W" ,
                                sample.decode('utf-8')
                               )
        else:
            data = parse.search("Dm={3D}D," +
                                "Sm={.1F}M," +
                                "Ta={.1F}C," +
                                "Ua={.1F}P," +
                                "Pa={.1F}H," +
                                "Rc={.2F}M," +
                                "Th={.1F}C," +
                                "Vh={.1F}#" ,
                                sample.decode('utf-8')
                               )
        # Can't figure out why I can't format parse class
        strip = [float(var) for var in data]
        ndict = dict(zip(parms, strip))

    elif sample.startswith(b'0R1'):
        parms = ['Dn', 'Dm', 'Dx', 'Sn', 'Sm', 'Sx']
        data = parse.search("Dn={3D}D," +
                            "Dm={3D}D," +
                            "Dx={3D}D," +
                            "Sn={.1F}M," +
                            "Sm={.1F}M," +
                            "Sx={.1F}M" ,
                            sample.decode('utf-8')
                            )
        # Can't figure out why I can't format parse class
        strip = [float(var) for var in data]
        ndict = dict(zip(parms, strip))
        

    elif sample.startswith(b'0R2'):
        parms = ['Ta', 'Ua', 'Pa']
        data = parse.search("Ta={.1F}C," +
                            "Ua={.1F}P," +
                            "Pa={.1F}H" ,
                            sample.decode('utf-8')
                            )
        # Can't figure out why I can't format parse class
        strip = [float(var) for var in data]
        ndict = dict(zip(parms, strip))

    elif sample.startswith(b'0R3'):
        parms = ['Rc', 'Rd', 'Ri', 'Hc', 'Hd', 'Hi']
        data = parse.search("Rc={.2F}M," +
                            "Rd={.2F}s," +
                            "Ri={.2F}M," +
                            "Hc={.2F}M," +
                            "Hd={.2F}s," +
                            "Hi={.2F}M" ,
                            sample.decode('utf-8')
                            )
        # Can't figure out why I can't format parse class
        strip = [float(var) for var in data]
        ndict = dict(zip(parms, strip))

    # note: this should be 0R5, but funky values without heater plugged in
    elif sample.startswith(b'0R9'):
        parms = ['Th', 'Vh', 'Vs', 'Vr']
        data = parse.search("Th={.1F}C," +
                            "Vh={.1F}N," +
                            "Vs={.1F}V," +
                            "Vr={.3F)}V" ,
                            sample.decode('utf-8')
                            )
        if data is None:
            data = parse.search("Th={.1F}C," +
                                "Vh={.1F}#," +
                                "Vs={.1F}V," +
                                "Vr={.3F)}V" ,
                                sample.decode('utf-8')
                               )
        # Can't figure out why I can't format parse class
        strip = [float(var) for var in data]
        ndict = dict(zip(parms, strip))

    else:
        ndict = None
                 
    return ndict


def start_publishing(args, plugin, dev, query, **kwargs):
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
    # Define the timestamp
    timestamp = get_timestamp()
    # Note: WXT interface commands located within manual
    # Note: query command sent to the instrument needs to be byte
    dev.write(bytearray(query + '\r\n', 'utf-8'))
    line = dev.readline()
    # Remove all leading/trailing checksum characters
    newstring = b''.join(bytes([byte]) for byte in line if byte  > 14)
    # Check for valid command
    sample = parse_values(newstring)
    # check for debug; output direct from the instrument
    if kwargs['debug'] == True:
        print(datetime.datetime.fromtimestamp(timestamp / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f'), line)
        print(newstring)
    # If valid parsed values, send to publishing
    if sample:
        # setup and run publishing schedule
        if kwargs['node_interval'] > 0:
            # publish each value in sample
            for name, key in kwargs['names'].items():
                try:
                    value = sample[key]
                except KeyError:
                    continue
                # Publish to the node
                plugin.publish(name,
                               value=value,
                               meta={"units" : kwargs['units'][name],
                                     "sensor" : "vaisala-wxt536",
                                     "missing" : "-9999.9",
                                    },
                               scope="node",
                               timestamp=timestamp
                              )
                                    
        if kwargs['beehive_interval'] > 0:
            # publish each value in sample
            for name, key in kwargs['names'].items():
                try:
                    value = sample[key]
                except KeyError:
                    continue
                # Update the log
                plugin.publish(name,
                               value=value,
                               meta={"units" : kwargs['units'][name],
                                     "sensor" : "vaisala-wxt536",
                                     "missing" : "-9999.9"
                                    },
                               scope="beehive",
                               timestamp=timestamp
                              )

def main(args):
    publish_names = {"wxt.wind.direction" : "Dm",
                     "wxt.wind.speed" : "Sm",
                     "wxt.env.temp" : "Ta",
                     "wxt.env.humidity" : "Ua",
                     "wxt.env.pressure" : "Pa",
                     "wxt.rain.accumulation" : "Rc",
                     "wxt.rain.duration" : "Rd",
                     "wxt.rain.intensity" : "Ri",
                     "wxt.rain.peak" : "Rp",
                     "wxt.hail.accumulation" : "Hc",
                     "wxt.hail.duration" : "Hd",
                     "wxt.hail.intensity" : "Hi",
                     "wxt.hail.peak" : "Hp",
                     "wxt.heater.temp" : "Th",
                     "wxt.heater.volt" : "Vh",
                     "wxt.voltage.supply" : "Vs",
                     "wxt.voltage.reference" : "Vr"
                    }

    units = {"wxt.wind.direction" : "degrees",
             "wxt.wind.speed" : "meters per second",
             "wxt.env.temp" : "degree Celsius",
             "wxt.env.humidity" : "percent",
             "wxt.env.pressure" : "hectoPascal",
             "wxt.rain.accumulation" : "milimeters",
             "wxt.rain.duration" : "seconds",
             "wxt.rain.intensity" : "millimeters per hour",
             "wxt.rain.peak" : "millimeters per hour",
             "wxt.hail.accumulation" : "hits per square centimeter",
             "wxt.hail.duration" : "seconds",
             "wxt.hail.intensity" : "hits per square centimeter per hour",
             "wxt.hail.peak" : "hits per square centimeter per hour",
             "wxt.voltage.supply" : "volts",
             "wxt.heater.temp" : "degree Celsius",
             "wxt.heater.volt" : "volts",
             "wxt.voltage.reference" : "volts"
             }
    
    with Plugin() as plugin, serial.Serial(args.device, baudrate=args.baud_rate, timeout=1.0) as dev:
        while True:
                start_publishing(args, 
                                 plugin,
                                 dev,
                                 args.query,
                                 node_interval=args.node_interval,
                                 beehive_interval=args.beehive_interval,
                                 names=publish_names,
                                 units=units,
                                 debug=args.debug
                                )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description="Plugin for Pushing Viasala WXT 2D anemometer data through WSN")

    parser.add_argument("--debug",
                        type=bool,
                        default=False,
                        dest='debug',
                        help="enable debug logs"
                        )
    parser.add_argument("--device",
                        type=str,
                        dest='device',
                        default="/dev/ttyUSB7",
                        help="serial device to use"
                        )
    parser.add_argument("--baudrate",
                        type=int,
                        dest='baud_rate',
                        default=19200,
                        help="baudrate to use"
                        )
    parser.add_argument("--node-publish-interval",
                        default=-1.0,
                        dest='node_interval',
                        type=float,
                        help="interval to publish data to node " +
                             "(negative values disable node publishing)"
                        )
    parser.add_argument("--beehive-publish-interval",
                        default=1.0,
                        dest='beehive_interval',
                        type=float,
                        help="interval to publish data to beehive " +
                             "(negative values disable beehive publishing)"
                        )
    parser.add_argument("--query",
                        type=str,
                        default="0R0",
                        help="ASCII query command to send to the instrument"
                       )
    args = parser.parse_args()


    main(args)
