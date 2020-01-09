import paho.mqtt.client as mqtt
import datetime
import smtplib
import json
import psycopg2

from apscheduler.schedulers.background import BackgroundScheduler
# from email.MIMEMultipart import MIMEMultipart
from email.mime.multipart import MIMEMultipart

from email.mime.text import MIMEText

from configparser import ConfigParser

import time
import logging

logging.basicConfig()

# global database_connected
database_connected = False
mqtt_connected = False


# global conn

def debug(_subj, _message):
    print(_subj + ":" + _message)
    pass


def create_message(from_s, recipients, subject, body):
    msg = MIMEMultipart()
    # msg['From'] = 'lab240 notifyer'
    msg['From'] = from_s
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body))
    return msg


def send_mail(user, from_s, password, recipients, subject, body):
    msg = create_message(from_s, recipients, subject, body)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(user, password)
    server.sendmail(user, recipients, msg.as_string())
    server.close()
    print('Sent email to %s' % (', '.join(recipients)))


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    global mqtt_connected

    if rc != 0:
        mqtt_connected = False
        return

    mqtt_connected = True

    debug("SYS", str("Connected with result code " + str(rc)))
    # client.subscribe("/donoff/b1/out/info")
    dt = datetime.datetime.now()
    s_time_command = "time=" + str(dt.hour) + ":" + str(dt.minute);
    # print("D:"+s_time_command);
    # ret=client.publish("/donoff/b1/in/params", s_time_command);

    client.subscribe(TOPIC_SEMDMAIL)
    client.subscribe(TOPIC_SENSOR_BASELOG)
    client.subscribe(TOPIC_ALIVE)


def on_disconnect(client, userdata, rc):
    debug("MQTTDISCONNECT", "DISCONNECT")
    global mqtt_connected
    mqtt_connected = False


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # print(msg.topic + "-->" + str(msg.payload))
    if msg.retain == 1:
        debug("MQTTPARSER", "Skip retain message")
        return

    if msg.topic == TOPIC_SEMDMAIL:
        debug("MQTTPARSER", "lets parse sendmail")
        parsed_sendmail = json.loads(msg.payload)

        # print("dev="+parsed_sendmail["dev"]);
        to = [""]
        to[0] = parsed_sendmail["email"]

        subj = "notify dev:" + parsed_sendmail["dev"] + "->" + parsed_sendmail["subj"]
        body = parsed_sendmail["subj"] + "\n" + "dev:" + parsed_sendmail["dev"] + '\n' + parsed_sendmail[
            "body"] + "\n\n\n" + "time=" + str(datetime.datetime.now())
        print("Lets notify sendmail")
        send_mail(gmail_login, from_str, gmail_pass, to, subj, body)

    if msg.topic == TOPIC_ALIVE:
        # debug("MQTTPARSER", "lets parse alive")
        parsed_alive = json.loads(msg.payload)
        # print(parsed_alive["dev"] + ":" + "alive!")
        # write alive to base !

    if msg.topic == TOPIC_SENSOR_BASELOG:

        parsed_sensor_data = json.loads(msg.payload)
        # print(parsed_sensor_data)
        # debug("MQTTPARSER", str("baselog ask from:" + parsed_sensor_data['dev']))
        if conn:
            # print ("!!")
            debug("BASELOG", str('Insert to sensor log database ' + parsed_sensor_data['dev'] + ':'
                                 + parsed_sensor_data['user'] + ':' + parsed_sensor_data['name'] + ':'
                                 + str(parsed_sensor_data['val'])))

            insert_sensor_log(conn, parsed_sensor_data)

        else:
            debug("BASELOG", "No Connection to database, SKIP")
            pass

    pass


def on_log(mosq, obj, mid, string):
    # print("Log: " + str(string))
    pass


def on_publish(mosq, obj, mid):  # create function for callback
    # print("mid: " + str(mid))
    pass


def tick():
    _str = ''
    if mqtt_connected:
        _str += "MQTT OK"
    else:
        _str += "MQTT DISCONNECTED"

    _str += ", "
    if database_connected:
        _str += "DATABASE OK"
    else:
        _str += "DATABASE DISCONNECTED"

    debug("TICK", _str)


def connect_database():
    try:
        print('Lets database connect...')
        connection = psycopg2.connect(dbname=DBNAME, user=DBUSER, password=DBPASS, host=DBHOST, port=DBPORT,
                                      connect_timeout=3)
        print("Connection ok")
        return connection
    except psycopg2.DatabaseError as err:
        print('Connection Error')
        print(str(err))


def insert_sensor_log(c, data):
    with c.cursor() as cursor:
        cursor.execute("""INSERT INTO tmp_lab240 (time, duser, device, sensortype, sensorname,value, multiplier) 
            VALUES (%s, %s, %s, %s, %s, %s, %s) """, (datetime.datetime.now(), data['user'], data['dev'],
                                                      data['s_type'], data['name'], data['val'], data['mult']))
    # cursor.execute("INSERT INTO tmp_lab240_2 (id) VALUES (%s);", [7])
    c.commit()


def insert_events_log(c, data):
    with c.cursor() as cursor:
        cursor.execute("""INSERT INTO tmp_lab240_eventslog (time, duser, device, event_type, reason, value, multiplier) 
            VALUES (%s, %s, %s, %s, %s, %s, %s) """, (datetime.datetime.now(), data['user'], data['dev'],
                                                      data['s_type'], data['name'], data['val'], data['mult']))
    # cursor.execute("INSERT INTO tmp_lab240_2 (id) VALUES (%s);", [7])
    c.commit()


def reconnect_base():
    # print('*************** Base connection OK')
    global conn
    global database_connected
    if not conn:
        debug("SYS", 'Try reconnect to database ...')
        database_connected = False
        conn = connect_database()
        if conn:
            debug("SYS", 'Database conn OK ...')
            database_connected = True


def reconnect_mqtt():
    # global client
    # if not (client.isConnected() ):
    #     debug("SYS", "MQTT NOT connected")
    if mqtt_connected:
        debug("SYS", "MQTT OK")
    else:
        debug("SYS", "MQTT NOT Connected")
    pass


def reconnect():
    reconnect_base()
    reconnect_mqtt()


data_file = 'conf.donoff'

debug('SYS', 'Starting Donoff python broker')

config = ConfigParser()
config.read(data_file)

try:
    mqtt_conf = config['mqtt']
    email_conf = config['email']
    sql_conf = config['sql']

    gmail_login = email_conf['gmail_login']
    gmail_pass = email_conf['gmail_pass']
    from_str = email_conf['from_str']

    TOPIC_SEMDMAIL = "/sys/sendmail"
    TOPIC_SENSOR_BASELOG = "/sys/sensor_baselog"
    TOPIC_ALIVE = "/sys/alive"

    DBNAME = sql_conf['name']
    DBUSER = sql_conf['user']
    DBPASS = sql_conf['pass']
    DBHOST = sql_conf['host']
    DBPORT = sql_conf['port']

    mqtt_login = mqtt_conf['login']
    mqtt_pass = mqtt_conf['password']
    mqtt_server = mqtt_conf['server']
    mqtt_port = mqtt_conf['port']

except KeyError:
    debug("SYS", "Error config file")
    raise SystemExit(1)

conn = connect_database()

if conn:
    database_connected = True

client = mqtt.Client("mypython")
client.username_pw_set(username=mqtt_login, password=mqtt_pass)

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.on_log = on_log
client.on_publish = on_publish

try:
    debug("SYS", 'Connecting to server=' + str(mqtt_server) + ' port=' + str(mqtt_port) + ' login=' + str(
        mqtt_login) + ' pass=' + mqtt_pass)
    client.connect(mqtt_server, port=int(mqtt_port), keepalive=10)
    mqtt_connected = True
    debug("SYS", "MQTT connected OK")
except:
    mqtt_connected = False
    debug("SYS", "MQTT connected FALSE")

client.loop_start()
scheduler = BackgroundScheduler()
scheduler.add_job(tick, 'interval', seconds=10)
scheduler.add_job(reconnect, 'interval', seconds=30)
scheduler.start()

while True:
    # print("publish")
    time.sleep(1)  # sleep for 10 seconds before next call
