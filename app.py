import serial
import argparse
import parse
import logging
import datetime

from waggle.plugin import Plugin, get_timestamp

def parse_values(sample, **kwargs):
    # Set a default
    ndict = None
    # Note: Specific to WXT ASCII query commands
    if sample.startswith(b'0R0'):
        # ASCII sting changes voltage heater character if 
        # voltage is supplied or not.
        #   - '#' for voltage not supplied  
        #          - assigned value - 0
        #   - 'N' for supplied voltage and above heating temp 
        #          - assigned value - 1
        #   - 'V' heating is on at 50% duty cycle, between high and middle control
        #          - assigned value - 2
        #   - 'W' heating is on 100% duty cycle, between low and middle control temps
        #          - assigned value - 3
        #   - 'F' heating is on at 50% duty cycle, heating temp below low control temp
        #          - assigned value - 4
        # The heater character is the last value, skip for now.
        data = parse.search("Dm={:d}D," +
                    "Sm={:f}M," +
                    "Ta={:f}C," +
                    "Ua={:f}P," +
                    "Pa={:f}H," +
                    "Rc={:f}M," +
                    "Hc={:f}M," +
                    "Th={:f}C," +
                    "Vh={:f}",
                    sample.decode('utf-8')
        )
        if data:
            parms = ['Dm', 'Sm', 'Ta', 'Ua', 'Pa', 'Rc', 'Hc', 'Th', 'Vh']
            # Convert to a list to convert from parse result object
            strip = [float(var) for var in data]
            ndict = dict(zip(parms, strip))
            # Search for remaining heating options
            if parse.search("Vs={:f}V,", sample.decode('utf-8')):
                ndict.update({'Vs' : [float(var) for var in parse.search("Vh={:f}V", sinput.decode('utf-8'))][0]})
            if parse.search("Vr={:f}V,", sample.decode('utf-8')):
                ndict.update({'Vr' : [float(var) for var in parse.search("Vh={:f}V", sinput.decode('utf-8'))][0]})
            # Apply the heater status to the dictionary
            if parse.search("Vh={:f}N", sample.decode('utf-8')):
                ndict.update({'Jo' : 1})
            elif parse.search("Vh={:f}V", sample.decode('utf-8')):
                ndict.update({'Jo' : 2})
            elif parse.search("Vh={:f}W", sample.decode('utf-8')):
                ndict.udpate({'Jo' : 3})
            elif parse.search("Vh={:f}F", sample.decode('utf-8')):
                ndict.update({'Jo' : 5})
            else:
                ndict.update({'Jo' : 0})         
        else:
            # The WXT summary command is user-defined. 
            # Thus, WXT may not have same summary configuration as the CROCUS nodes.
            # Define the faculty default for the summary command.
            # The heater character is the last value, skip for now.
            data = parse.search("Dm={:d}D," +
                                "Sm={:f}M," +
                                "Ta={:f}C," +
                                "Ua={:f}P," +
                                "Pa={:f}H," +
                                "Rc={:f}M," +
                                "Th={:f}C," +
                                "Vh={:f}" ,
                                sample.decode('utf-8')[:-1]
            )
            if data:
                parms = ['Dm', 'Sm', 'Ta', 'Ua', 'Pa', 'Rc', 'Th', 'Vh']
                # Convert to a list to convert from parse result object
                strip = [float(var) for var in data]
                ndict = dict(zip(parms, strip))
                # Apply the heater status to the dictionary
                if sample.decode('utf-8')[-1] == 'N':
                    ndict.update({'Jo' : 1})
                elif sample.decode('utf-8')[-1] == 'V':
                    ndict.update({'Jo' : 2})
                elif sample.decode('utf-8')[-1] == 'W':
                    ndict.udpate({'Jo' : 3})
                elif sample.decode('utf-8')[-1] == 'F':
                    ndict.update({'Jo' : 5})
                else:
                    ndict.update({'Jo' : 0})     

    elif sample.startswith(b'0R1'):
        parms = ['Dn', 'Dm', 'Dx', 'Sn', 'Sm', 'Sx']
        data = parse.search("Dn={:d}D," +
                            "Dm={:d}D," +
                            "Dx={:d}D," +
                            "Sn={:f}M," +
                            "Sm={:f}M," +
                            "Sx={:f}M" ,
                            sample.decode('utf-8')
                            )
        if data:
            # Can't figure out why I can't format parse class
            strip = [float(var) for var in data]
            ndict = dict(zip(parms, strip))
        
    elif sample.startswith(b'0R2'):
        parms = ['Ta', 'Ua', 'Pa']
        data = parse.search("Ta={:f}C," +
                            "Ua={:f}P," +
                            "Pa={:f}H" ,
                            sample.decode('utf-8')
                            )
        if data:
            # Can't figure out why I can't format parse class
            strip = [float(var) for var in data]
            ndict = dict(zip(parms, strip))

    elif sample.startswith(b'0R3'):
        parms = ['Rc', 'Rd', 'Ri', 'Hc', 'Hd', 'Hi']
        data = parse.search("Rc={:f}M," +
                            "Rd={:f}S," +
                            "Ri={:f}M," +
                            "Hc={:f}M," +
                            "Hd={:f}S," +
                            "Hi={:f}M" ,
                            sample.decode('utf-8')
                            )
        if data:
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
    # check for debug; output direct from the instrument
    if kwargs['debug'] == True:
        print(datetime.datetime.fromtimestamp(timestamp / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f'), line)
        print(newstring)
    # Check for valid command
    sample = parse_values(newstring)
    # If valid parsed values, send to publishing
    if sample:
        # Define a list to hold the additional meta data for the heater
        heater_info = ["Heating Voltage Not Supplied",
                       "Heating Voltage Supplied and Above Heating Temperature Threshold",
                       "Heating Voltage Supplied and is between High and Middle Control Temperature Threshold",
                       "Heating Voltage Supplied and is between Low and Middle Control Temperature Threshold",
                       "Heating Voltage SUpplied and is Below Low Control Temperature Threshold"]
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
                if key == 'Jo':
                    plugin.publish(name,
                                   value=value,
                                   meta={"units" : kwargs['units'][name],
                                         "sensor" : "vaisala-wxt536",
                                         "missing" : "-9999.9",
                                         "status" : heater_info[value]
                                    },
                                    scope="beehive",
                                    timestamp=timestamp
                                    )
                else:
                    plugin.publish(name,
                                   value=value,
                                   meta={"units" : kwargs['units'][name],
                                         "sensor" : "vaisala-wxt536",
                                         "missing" : "-9999.9",
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
                     "wxt.voltage.reference" : "Vr",
                     "wxt.heater.status" : "Jo"
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
             "wxt.heater.status" : "unitless",
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
