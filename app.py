"""
This module interfaces with the Vaisala WXT536 and parses the output of the
instrument before uploading to Beehive via Waggle. 

To display currently available serial ports:
python -m serial.tools.list_ports
"""

from random import sample
import time
import serial
import argparse
import parse
import csv
from pathlib import Path
import threading
import pandas as pd
import xarray as xr

from datetime import datetime, timezone
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
                ndict.update({'Vs' : [float(var) for var in parse.search("Vs={:f}V", sample.decode('utf-8'))][0]})
            if parse.search("Vr={:f}V,", sample.decode('utf-8')):
                ndict.update({'Vr' : [float(var) for var in parse.search("Vr={:f}V", sample.decode('utf-8'))][0]})
            # Apply the heater status to the dictionary
            if parse.search("Vh={:f}N", sample.decode('utf-8')):
                ndict.update({'Jo' : 1})
            elif parse.search("Vh={:f}V", sample.decode('utf-8')):
                ndict.update({'Jo' : 2})
            elif parse.search("Vh={:f}W", sample.decode('utf-8')):
                ndict.update({'Jo' : 3})
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
                                "Vh={:f}",
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
                    ndict.update({'Jo' : 3})
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
                            "Sx={:f}M",
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
                            "Pa={:f}H",
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
                            "Hi={:f}M",
                            sample.decode('utf-8')
                            )
        if data:
            # Can't figure out why I can't format parse class
            strip = [float(var) for var in data]
            ndict = dict(zip(parms, strip))

    else:
        ndict = None

    return ndict

def list_files(img_dir):
    """
    Lists all files within a directory and their sizes in bytes.

    Parameters:
        img_dir: The path to the directory to list files from within
            the DockerFile image.
    """
    dir_path = Path(img_dir)
    saved_files = sorted(list(dir_path.glob("*.csv")))
    if saved_files:
        print(f'\nUpdated local files within {dir_path}:')
        for sfile in saved_files:
            file_size = sfile.stat().st_size
            print(f"{sfile}: {file_size} bytes")

def secs_to_xr_freq(seconds):
    """cleanly convert seconds to a string frequency for xarray resampling"""
    seconds = int(seconds) * 60
    if seconds <= 0:
        raise ValueError("seconds must be > 0")

    # prefer larger, cleaner units when possible
    if seconds % 3600 == 0:
        return f"{seconds // 3600}H"
    if seconds % 60 == 0:
        return f"{seconds // 60}min"
    return f"{seconds}s"

def initialize_local_file(site, outdir, publish_names):
    """Function to generate the filename and header info for local file"""
    nout = (site +
            '.wxt536.' +
            datetime.now(timezone.utc).strftime("%Y%m%d.%H%M%S") +
            '.csv')
    # Define the Path to the CSV file
    csv_path = Path(outdir) / nout
    # Ensure the parent directory exists
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize the CSV file with headers
    print(f"Initializing local CSV file at {csv_path}")
    with open(csv_path, mode='w', newline='', encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write the header row
        header = ['Timestamp'] + [info[1] for info in publish_names.values()]
        units = ['UTC seconds'] + [info[2] for info in publish_names.values()]
        waggle_vars = ['Timestamp'] + [info[0] for info in publish_names.values()]
        short_names = ['time'] + [info for info in publish_names.keys()]

        csv_writer.writerow(header)
        csv_writer.writerow(units)
        csv_writer.writerow(waggle_vars)
        csv_writer.writerow(short_names)

    return csv_path

def publish_file(file_path):
    """Utilizing threading, publish file to Beehive"""
    def upload_file(file_path):
        """Call the Waggle Plugin"""
        with Plugin() as plugin:
            plugin.upload_file(file_path, timestamp=get_timestamp())
            print(f"Published {file_path}")
    # Define threads
    thread = threading.Thread(target=upload_file, args=(file_path,))
    thread.start()
    thread.join()

def publish_avg(arg, file_path, publish_names):
    """
    Calculate a user define average from the local data files
    and publish to Beehive. 
    """
    # Define the timestamp
    timestamp = get_timestamp()

    # Define a list to hold the additional meta data for the heater
    heater_info = ["Heating Voltage Not Supplied",
                   "Heating Voltage Supplied and Above Heating Temperature Threshold",
                   "Heating Voltage Supplied and is between High and Middle Control Temperature Threshold",
                   "Heating Voltage Supplied and is between Low and Middle Control Temperature Threshold",
                   "Heating Voltage Supplied and is Below Low Control Temperature Threshold"]

    df = pd.read_csv(file_path, skiprows=3, na_values=-9999)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce").dt.tz_convert(None)
    df = df.set_index("time").sort_index()
    ds = xr.Dataset.from_dataframe(df)

    ds = ds.assign_coords(time=pd.to_datetime(ds["time"].values))

    # define temporal frequency for resampling
    nfreq = secs_to_xr_freq(arg.beehive_interval)

    # Temporal mean for everything except rainfall accumulation
    ds_mean = ds.drop_vars(["Rc"]).resample(time=nfreq).mean()

    # put it back into one Dataset
    ds_mean['Rc'] = [ds.Rc.data[-1]]

    ## -- Publish Parsed and Averaged Telegram to Beehive ---
    # publish each value in sample
    with Plugin() as plugin:
        for name, key in publish_names.items():
            try:
                value = ds_mean[name].data[-1]
            except KeyError:
                continue
            # Update the log
            if key == 'Jo':
                plugin.publish(key[0],
                               value=value,
                               meta={"units" : key[2],
                                     "sensor" : "vaisala-wxt536",
                                     "missing" : "-9999.9",
                                     "status" : heater_info[value],
                                     "avg_frequency" : nfreq
                                },
                                scope="beehive",
                                timestamp=timestamp
                )
            else:
                plugin.publish(key[0],
                               value=value,
                               meta={"units" : key[2],
                                     "sensor" : "vaisala-wxt536",
                                     "missing" : "-9999.9",
                                     "avg_frequency" : nfreq},
                               scope="beehive",
                               timestamp=timestamp
                )
    # cleanup
    del df, ds, ds_mean

def query(args, ser, publish_names, **kwargs):
    """
    Sends query command to the WXT536 instrument, parses the returned
    telegram, and publishes to Beehive via Waggle Plugin.

    Additionally, writes the raw data to a local file if specified.
    """
    # Define the timestamp
    timestamp = get_timestamp()

    ## -- Query the WXT and Parse the Returned Telegram ----

    # Note: WXT interface commands located within manual
    # Note: query command sent to the instrument needs to be byte
    ser.write(bytearray(args.query + '\r\n', 'utf-8'))
    line = ser.readline()
    # Remove all leading/trailing checksum characters
    newstring = b''.join(bytes([byte]) for byte in line if byte  > 14)
    # check for debug; output direct from the instrument
    if args.debug == True:
        print('Raw Output from WXT536:')
        print(datetime.fromtimestamp(timestamp / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f'), line)
        print(newstring)
    # Check for valid command
    sample = parse_values(newstring)
    if args.debug == True:
        print(f"Parsed Sample: {sample}")

    # If valid parsed values, send to publishing
    if sample:
        ## -- Write to Local File if Specified ----
        if 'local_file' in kwargs and kwargs['local_file']:
            with open(kwargs['local_file'], mode='a', newline='', encoding="utf-8") as csvfile:
                csv_writer = csv.writer(csvfile)
                ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
                out_values = [str(sample.get(val, '-9999')) for val in publish_names.keys()]
                csv_writer.writerow([ts, *out_values])
                csvfile.flush()

def main(args):
    """Main function for WXT536 interface and publishing"""
    publish_names = {"Dm" : ["wxt.wind.direction", "Mean Wind Direction", "degrees"],
                     "Sm" : ["wxt.wind.speed", "Mean Wind Speed", "m/s"],
                     "Ta" : ["wxt.env.temp", "Air Temperature", "C"],
                     "Ua" : ["wxt.env.humidity", "Relative Humidity", "%"],
                     "Pa" : ["wxt.env.pressure", "Atmospheric Static Air Pressure", "hPa"],
                     "Rc" : ["wxt.rain.accumulation", "Rain Accumulation", "mm"],
                     "Rd" : ["wxt.rain.duration", "Rain Duration", "s"],
                     "Ri" : ["wxt.rain.intensity", "Rain Intensity", "mm/h"],
                     "Rp" : ["wxt.rain.peak", "Rain Peak Intensity", "mm/h"],
                     "Hc" : ["wxt.hail.accumulation", "Hail Accumulation", "mm"],
                     "Hd" : ["wxt.hail.duration", "Hail Duration", "s"],
                     "Hi" : ["wxt.hail.intensity", "Hail Intensity", "mm/h"],
                     "Hp" : ["wxt.hail.peak", "Hail Peak Intensity", "mm/h"],
                     "Th" : ["wxt.heater.temp", "Heater Temperature", "C"],
                     "Vh" : ["wxt.heater.volt", "Heater Voltage", "V"],
                     "Vs" : ["wxt.voltage.supply", "Supply Voltage", "V"],
                     "Vr" : ["wxt.voltage.reference", "Reference Voltage", "V"],
                     "Jo" : ["wxt.heater.status", "Heater Status", "Unitless"]
                    }

    with serial.Serial(args.device,
					   args.baud_rate,
					   parity=serial.PARITY_NONE,
					   stopbits=serial.STOPBITS_ONE,
					   bytesize=serial.EIGHTBITS,
					   timeout=1) as ser:
        try:
            print(f"Serial connection to {args.device} is open")
            last_timestamp = time.gmtime()

            # ---- Local File Initialization ----
            # Check to see if data are written to local file for upload
            if args.beehive_interval > 0:
                print(f"Writing data to local file. New file generates every {args.beehive_interval} seconds")
                # Define the filename
                nfile_writer = initialize_local_file(args.site, args.outdir, publish_names)

            # if desired, check on current files and file sizes
            if args.debug == True:
                # check on the files
                list_files(args.outdir)
                print("\n")

            # --- Main WXT Interface Loop ----
            while True:

                # --- Check on Local File Creation Interval ----
                if args.beehive_interval > 0:
                    current_timestamp = time.gmtime()

                    if (current_timestamp.tm_min % args.beehive_interval == 0
                            and current_timestamp.tm_min != last_timestamp.tm_min):
                        ## -- Publish Parsed Telegram to Beehive ---
                        if args.beehive_interval > 0:
                            publish_avg(args, nfile_writer.name, publish_names)
                        # Close the current file and create a new one
                        if nfile_writer:
                            print(f"Closing {nfile_writer.name}")
                            publish_file(nfile_writer.name)
                        # Intialize a new local file
                        nfile_writer = initialize_local_file(args.site, args.outdir, publish_names)
                        last_timestamp = current_timestamp

                ## --- Verify Serial Connection ----
                # Check the serial connection. If not defined, re-establish.
                if ser is None:
                    ser = serial.Serial(args.device,
					                    args.baud_rate,
					                    parity=serial.PARITY_NONE,
					                    stopbits=serial.STOPBITS_ONE,
					                    bytesize=serial.EIGHTBITS,
					                    timeout=1)
                    print(f"Reconnecting Serial Connection with {args.device}")

                ## --- Begin Data Publishing ----
                # Begin - parse telegram
                if args.beehive_interval > 0:
                    query(args,
                          ser,
                          publish_names,
                          local_file=nfile_writer,
                    )
                else:
                    query(args,
                          ser,
                          publish_names
                    )

                ## -- Query Interval Wait ---
                if isinstance(args.query_interval, (int, float)) and args.query_interval > 0:
                    time.sleep(args.query_interval)
                else:
                    print("Invalid query interval, defaulting to 1 second")
                    time.sleep(1)

        except KeyboardInterrupt:
            print(f"Program interrupted, closing serial port {args.device}")
        finally:
            if ser:
                ser.close()

if __name__ == '__main__':

    plugin_descript = ("Script for interfacing with the Vaisala WXT536 datastream." +
                       " Publishes data to Sage Beehive as immediate or averaged " +
                       " observations, while providing files of raw observations " +
                       " at user selected frequency."
    )

    plugin_usage = ("python app.py --debug True --behive-publish-interval 60")

    parser = argparse.ArgumentParser(description=plugin_descript,
                                     usage=plugin_usage)

    parser.add_argument("--debug",
                        type=bool,
                        default=False,
                        dest='debug',
                        help="[Boolean|Default False] Enable Output from Serial"
                             " Communication to Screen for Debugging"
                        )
    parser.add_argument("--device",
                        type=str,
                        dest='device',
                        default="/dev/ttyUSB7",
                        help="[str|Default /dev/ttyUSB7] Serial Device to" +
                             " Establish Serial Communication"
                        )
    parser.add_argument("--baudrate",
                        type=int,
                        dest='baud_rate',
                        default=19200,
                        help="[int|Default 19200] Serial Communication Baudrate"
                        )
    parser.add_argument("--query",
                        type=str,
                        default="0R0",
                        dest="query",
                        help="[str|Default 0R0] ASCII query command to send" +
                             " to the instrument"
                       )
    parser.add_argument("--query-interval",
                        type=int,
                        default=1,
                        dest="query_interval",
                        help="[int|Default 1sec] WXT Query Frequency in seconds "
                       )
    parser.add_argument("--beehive-publish-interval",
                        default=15,
                        dest='beehive_interval',
                        type=int,
                        help="[float|Default 15 min] Interval to publish data to" +
                             " beehive (negative values disable beehive publishing)." +
                             " Values > query-interval will result in averaged data."
                        )
    parser.add_argument("--outdir",
                        type=str,
                        dest="outdir",
                        default=".",
                        help="[str| Default Current Working Directory] Directory where to output files to"
                        )
    parser.add_argument("--site",
                        type=str,
                        default="atmos",
                        dest="site",
                        help="[str | Default atmos] Site Identifer for Deployment location"
                        )
    args = parser.parse_args()


    main(args)
