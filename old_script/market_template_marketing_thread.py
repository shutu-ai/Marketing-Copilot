#在最开始就连接、修改 parts部分的逻辑，重连后还可以给这个用户发送
#长连接脚本
#不延迟发送
#限定发送时间段
#发送者是 telebirr
#发送完后，直接删掉
#优化：whitelist只读入一次
#修改switch_lan字段的值以控制不同语言发送

import sys
import time
import logging
import os
import threading
import pandas as pd
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.utils.dirsnapshot import DirectorySnapshot
import smpplib.gsm
import smpplib.client
import smpplib.consts
from datetime import datetime
import configparser

# 全局队列用于存放短信发送请求
sms_queue = Queue()


class SMSCConnection:
    def __init__(self, ip_add, port, system_id, password):
        self.ip_add = ip_add
        self.port = port
        self.system_id = system_id
        self.password = password
        self.client = None
        self.connected = False

    def connect(self):
        while not self.connected:
            try:
                self.client = smpplib.client.Client(self.ip_add, self.port, allow_unknown_opt_params=True)
                self.client.connect()
                self.client.bind_transceiver(system_id=self.system_id, password=self.password)
                self.connected = True
                logging.info("Connected to SMSC")
            except smpplib.exceptions.ConnectionError as e:
                logging.info(f"Connection failed: {e}, retrying in 5 seconds...")
                time.sleep(5)

    def disconnect(self):
        if self.connected:
            try:
                self.client.unbind()
                self.client.disconnect()
                self.connected = False
                logging.info("Disconnected from SMSC")
            except smpplib.exceptions.ConnectionError:
                self.connected = False

    def send_sms(self, sender, receiver, msg, campaign_name, cell_id):
        parts, encoding_flag, msg_type_flag = smpplib.gsm.make_parts(msg)
        #20240726发现1分钟不发就会被[Errno 104] Connection reset by peer，因此加入了重连机制，而且重连保证不漏发
        logging.info("entry send sms...%s",receiver)
        day_date=datetime.strftime(datetime.now(), "%Y%m%d")
        try:
            for part in parts:
                pdu = self.client.send_message(
                    source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
                    source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
                    source_addr=sender,
                    dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                    dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
                    destination_addr=receiver,
                    short_message=part,
                    data_coding=encoding_flag,
                    esm_class=msg_type_flag,
                    registered_delivery=True,
                )
        except smpplib.exceptions.ConnectionError:
            self.connected = False
            logging.info("Connection lost while sending message, reconnecting...")
            self.connect()
            for part in parts:
                pdu = self.client.send_message(
                    source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
                    source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
                    source_addr=sender,
                    dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                    dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
                    destination_addr=receiver,
                    short_message=part,
                    data_coding=encoding_flag,
                    esm_class=msg_type_flag,
                    registered_delivery=True,
                )
        with open(f"myfile_{day_date}.txt", "a") as file1:
            file1.write(f"{campaign_name}|{datetime.now()}|{receiver}|{cell_id}\n")


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, aim_path, smsc_connection):
        FileSystemEventHandler.__init__(self)
        self.aim_path = aim_path
        self.smsc_connection = smsc_connection

    def on_created(self, event):
        try:
            if event.src_path.endswith(".csv"):  # 检查是否是 CSV 文件
                time.sleep(1)
                modified_path = event.src_path.replace(".tmp", "").replace(".swp", "")
                file_time = datetime.strptime(modified_path.split("_")[-3], '%Y%m%d%H%M%S')
                current_time = datetime.now()
                # 定义工作时间范围（周一至周五的早上8点至下午5点30分）
                start_time = datetime(current_time.year, current_time.month, current_time.day, 8, 30)
                end_time = datetime(current_time.year, current_time.month, current_time.day, 17, 30)
                # 检查file_time是否在时间范围内
                in_scheduled_time = start_time <= file_time <= end_time and file_time.weekday() < 5
                #in_scheduled_time = start_time <= file_time <= end_time
                #logging.info(f"current_time: {current_time},start_time:{start_time}, end_time: {end_time}, in_scheduled_time: {in_scheduled_time}")
                # 如果在计划时间内，则继续走下去
                if in_scheduled_time:
                    try:
                        df = pd.read_csv(modified_path, sep='|')
                        df = df.loc[:, ["event_filter_id", "msisdn", "cell_id"]].drop_duplicates(["event_filter_id", "msisdn"])
                        # 在白名单的用户才发送
                        if os.path.exists("whitelist/whitelist.csv") and len(whitelist)>0:
                            #logging.info(f"get data {df}")
                            df = df[df['msisdn'].astype(str).isin(whitelist)]
                            #logging.info(f"current msisdn {df['msisdn']}")
                            logging.info("whitelist match msisdn count: %d",len(df['msisdn']))
                            if os.path.exists("exclusive/exclusive.csv"):
                                exclusive = pd.read_csv("exclusive/exclusive.csv").drop_duplicates("msisdn")
                                df = df[~df["msisdn"].astype(str).isin(exclusive["msisdn"].astype(str))]
                                logging.info("exclusive match msisdn count: %d",len(df["msisdn"]))
                            #是否按区域区分短信语言 1：区分，0：不区分
                            if switch_lan == 1:
                                #匹配语言维表
                                merged_df = pd.merge(df, dim_local_lan, how='left', left_on='cell_id', right_on='cgisai')
                                #如果匹配不到则默认ENG+Amharic
                                merged_df['Local_Language'].fillna('Amharic', inplace=True)
                                #选择需要的列
                                df = merged_df[['event_filter_id', 'msisdn', 'cell_id', 'Local_Language']]

                                for index, row in df.iterrows():
                                    try:
                                        # 将短信发送请求放入队列
                                        sms_queue.put((row["msisdn"], config[row["event_filter_id"]][row["Local_Language"]],
                                                       row["event_filter_id"], row["cell_id"]))
                                    except KeyError:
                                        logging.info(f"event_filter_id={row['event_filter_id']}不存在于配置文件中！")
                                        pd.DataFrame.from_records(
                                            [{"event_filter_id": row["event_filter_id"], "msisdn": row["msisdn"]}]).to_csv(
                                            "id_error/id_error_list.csv", mode="a", header=False, index=False)
                                        continue
                            else:
                                for index, row in df.iterrows():
                                    try:
                                        # 将短信发送请求放入队列
                                        sms_queue.put((row["msisdn"], config[row["event_filter_id"]]['campaign_message'],
                                                       row["event_filter_id"], row["cell_id"]))
                                        #logging.info(f"put msisdn {row['msisdn']} to queue, queue site {sms_queue.qsize()}")
                                    except KeyError:
                                        logging.info(f"event_filter_id={row['event_filter_id']}不存在于配置文件中！")
                                        pd.DataFrame.from_records(
                                            [{"event_filter_id": row["event_filter_id"], "msisdn": row["msisdn"]}]).to_csv(
                                            "id_error/id_error_list.csv", mode="a", header=False, index=False)
                                        continue

                            filename = modified_path.split('/')[-1].split('\\')[-1]
                            # 因为这是全量检测到符合条件的用户，量较大，所以不保存到sms_finished，直接删掉
                            #os.rename(filename, "sms_finished/" + filename)
                            os.remove(filename)
                        else:
                            logging.info('whitelist file not found or whitelist is empty')
                    except Exception as e:
                        logging.info(e)
                        pass
        except Exception as e:
            logging.info(e)
            pass

class DirMonitor(object):
    def __init__(self, aim_path, smsc_connection):
        self.aim_path = aim_path
        self.observer = Observer()
        self.smsc_connection = smsc_connection

    def start(self):
        event_handler = FileEventHandler(self.aim_path, self.smsc_connection)
        self.observer.schedule(event_handler, self.aim_path, True)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()

def read_config_file():
    config = configparser.RawConfigParser()
    config.read('../campaign_configuration.ini')
    return config

def sms_worker(smsc_connection):
    """短信发送工作线程"""
    while True:

        request = sms_queue.get()
        #logging.info(f"sms queue get info {request}")

        if request is None:
            break  # 收到终止信号，结束线程
        sender = 'telebirr'  # 举例：发送者信息
        receiver = "251" + str(request[0])  # 举例：接收者信息
        campaign_name = request[2]  # 从 request 中获取活动名称
        cell_id = request[3]  # 从 request 中获取单元 ID
        msg = request[1]
        # 发送短信
        smsc_connection.send_sms(sender, receiver, msg, campaign_name, cell_id)

        #time.sleep(0.05)  # 休眠 0.05 秒

if __name__ == "__main__":
     # 配置日志输出到文件和控制台
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("run.log"),  # 输出到文件
        ]
    )
    #控制是否按区域区分短信语言 1：区分，0：不区分
    switch_lan = 0
    if switch_lan == 1:
        dim_local_lan = pd.read_csv("../dim_local_lan.csv")
        logging.info("dim_local_lan_read")

    if not os.path.exists("sms_finished"):
        os.makedirs("sms_finished")
    if not os.path.exists("id_error"):
        os.makedirs("id_error")
    if not os.path.exists("id_error/id_error_list.csv"):
        pd.DataFrame(columns=['event_filter_id', 'msisdn']).to_csv("id_error/id_error_list.csv", index=False)

    config = read_config_file()

    # 在白名单的用户才发送,提前读入
    if os.path.exists("whitelist/whitelist.csv"):
        whitelist_temp = pd.read_csv("whitelist/whitelist.csv")
        whitelist = set(whitelist_temp['msisdn'].astype(str))
        logging.info('whitelist size: %d',len(whitelist))

    # 创建 SMSC 连接（首次就创建好，发短信过程中不创建）
    smsc_connection = SMSCConnection('10.204.181.73', 15019, '7127', 'Mak#1234')
    smsc_connection.connect()

    sms_thread = threading.Thread(target=sms_worker, args=(smsc_connection,))
    sms_thread.start()

    monitor = DirMonitor(".", smsc_connection)
    monitor.start()

    try:
        while True:
            #monitor_status()  # 执行监控函数
            time.sleep(1)
    except KeyboardInterrupt:
        # 发送终止信号给短信发送工作线程
        sms_queue.put(None)
        sms_thread.join() # 等待短信发送工作线程结束
        monitor.stop()
        smsc_connection.disconnect()
