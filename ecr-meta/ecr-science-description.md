# WXT536 Sampler
Waggle Plugin for the [Vaisala WXT536](https://www.vaisala.com/en/products/weather-environmental-sensors/weather-transmitter-wxt530-series#:~:text=Vaisala%20Weather%20Transmitter%20WXT530%20series,in%20a%20compact%2C%20affordable%20package) weather transmitter.
# Science
The Vaisala WXT536 is a multi-parameter weather sensor measures atmospheric temperatures, pressure, humdity, wind speed, wind direction, and precipitation. High quality atmospheric observations allow for understanding of the environmental conditions at the location of the instrument, and allow us to estimate transport of gas and particles through the atmosphere.

In combination with additional sensors attached to a CROCUS Level 1 Node, we are then able to evaluate the concentration of pollutants at the node, the transport of these pollutants into and out of the area of the node, and estimate removal of these pollutants from the atmosphere from preciptiation. Provides observations on meteorological conditions, including wind speed and direction, temperature, pressure, and precipitation estimate. 

# Usage
__Determine Serial Port__
PySerial offers a handy toolist to list all serial ports currently in use. 
To determine the port for the instrument, run

```bash
python -m serial.tools.list_ports
```
Otherwise, check `/tty/devUSB#` to see active ports. Default serial device is `/tty/devUSB1`


## Deployment 

Similar to the [Windsonic 2D Plugin](https://github.com/nikhil003/windsonic) a docker container will be setup via Makefile 

1.  Build the Container
```bash
make build
```

2.  Deploy the Container in Background
```bash
make deploy
```

3.  Test the plugin
```bash
make run
``` 

# Access the data
```py
import sage_data_client

df = sage_data_client.query(start="2023-04-10T12:00:00Z",
                            end="2023-04-10T15:00:00Z", 
                            filter={
                                "plugin": "10.31.81.1:5000/local/waggle-wxt536",
                                "vsn": "W057",
                                "sensor": "vaisala-wxt536"
                            }
)

```

Check [CROCUS Instrument Cookbooks](https://crocus-urban.github.io/instrument-cookbooks/notebooks/crocus_level1_node/viasala_wxt536.html) for details examples.

