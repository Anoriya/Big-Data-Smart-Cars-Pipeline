import csv
from time import sleep
from kafka import KafkaProducer


class CameraProducer:

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def start_streaming(self):
        with open("/home/sartharion/Bureau/5-mcluet/CAMERA/compressedAVI_v6.7.5_2_7-13_24_32-1280_800-30.avi.csv",
                  newline='') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                string = ';'.join(row) + "\n"
                self.producer.send('test', value=string.encode())
                print("sent")
                # sleep(1)