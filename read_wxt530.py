import serial
import time

from datetime import datetime

# To display current ports:
# python -m serial.tools.list_ports

def pollsave():
    # Initalize communication with the instrument
    #ser.write(b'0R0\r\n')
    ser.write(b'0R\r\n')
    # Read the incoming serial data
    data = ser.readline()
    # define a timestamp for the data and append to data string
    time = datetime.now().strftime("%Y%m%dT%H:%M:%S.%f")
    datatime = time + ','+ data.decode('utf-8')

    return datatime
 
if __name__ == '__main__':
    # start serial connection:
    with serial.Serial('/dev/ttyUSB3', 19200, timeout = 1) as ser:
        starttime = time.time()
        # create a file to write data to
        filename = 'WXT530_' + datetime.now().strftime("%Y%m%d.%H%M%S") + '.txt'
        while True:
            try:
                # poll the data
                ndata = pollsave()
                # Append to the file. 
                with open(filename, "a") as myfile:
                    try:
                        myfile.write(ndata)
                        print(ndata)
                    except:
                        print('Unable to write to file')                     
                time.sleep(0.20)
            except:
                print("Keyboard Interrupt")
                break

