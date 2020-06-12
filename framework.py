# ----------------------- #
# encoding: utf8          #
#                         #
# Authorized access only  #
#    By Joseph Wang       #
#                         #
#   cnjowang@gmail.com    #
# ----------------------- #
import hashlib
import json
import logging
import threading
import time
import urllib.parse

import paho.mqtt.client as mqtt
import redis
import requests

from proto.mm_pb2 import Message

logging.getLogger("requests").setLevel(logging.WARN)


class DuplicatedTeacherError(Exception): pass


class DuplicatedStudentError(Exception): pass


class LoginError(Exception): pass


class GeneralClassError(Exception): pass


logger = logging.getLogger("sgt")
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='a+')


class ServiceUtil:
    eto32Table = b"AB56DE3C8L2WF4UVM7JRSGPQYZTXK9HN"
    PRIVATE_KEY = "cc16be4b:346c51d"

    @classmethod
    def encode(cls, data, olen=26):
        s = 0
        ilen = len(data)
        result = list('\0' * olen)
        i = 0
        j = 0
        while i < ilen:
            b1 = ((data[i] << s) & 0xff) >> s
            b2 = data[i + 1] if i + 1 < ilen else 0
            if s >= 3:
                b1 = ((b1 & 0xff) << (s - 3)) | (((b2 & 0xff) >> (11 - s)) & 0xff)
                result[j] = cls.eto32Table[b1]
                j += 1
                s -= 3
            else:
                result[j] = cls.eto32Table[b1 >> (8 - s - 5)]
                j += 1
                s += 5
                i -= 1
            if j >= olen:
                break
            i += 1
        return bytes(result)

    @classmethod
    def sign(cls, *all_data):
        data = dict()
        for _data in all_data:
            data.update(_data)

        keys = list(data.keys())
        keys.sort()
        _s = []
        for _key in keys:
            _s.append("%s:%s" % (_key, data[_key]))
        _s.append(cls.PRIVATE_KEY)
        plain_text = ("".join(_s)).encode('utf8')
        digest = hashlib.md5(plain_text).digest()
        result = cls.encode(digest, 26)
        return result.decode('utf8')


class Endpoint(threading.Thread):
    def __init__(self, identity):
        super().__init__()
        self.data = None
        self.god = None
        self.identity = identity
        self.lock = threading.RLock()
        self.stop_sign = threading.Event()
        self.uid = None
        self.http_token = None
        self.mqtt_client = None
        self.mqtt_token = None
        self.mqtt_username = None
        self.topic_personal = None

    def stop(self):
        self.stop_sign.set()

    def _http_headers(self, data):
        headers = {
            "c": "1",
            "a": "1",
            "p": "2",
            "v": "v0.01",
            # "a": "RESERVED",
            "t": "%d" % int(time.time() * 1000),
        }
        if self.uid:
            headers['u'] = "%s" % self.uid
        if self.http_token:
            headers['token'] = self.http_token

        headers['s'] = ServiceUtil.sign(headers, data)
        return headers

    def _prepare(self):
        raise NotImplementedError()

    def publish(self, topic: str, message: Message):
        self.mqtt_client.publish(topic, message.SerializePartialToString())

    def run(self):
        self.t0 = time.time()
        try:
            self._prepare()
        except Exception as e:
            self.stop()
            self.god.endpoint_finish(self)
            logger.error("Error while starting %s: %s" % (self.identity, str(e)))
            return

        while not self.stop_sign.is_set():
            try:
                self.mqtt_client.loop()
            except Exception as e:
                logger.error("MQTT LOOP ERROR: " + str(e))
                break
        time.sleep(0.5)
        self.god.endpoint_finish(self)

    def __repr__(self):
        return "%s <%s>" % (self.identity, self.__class__.__name__)


class Teacher(Endpoint):
    def __init__(self, identity):
        super(Teacher, self).__init__(identity)
        self.http_session = requests.session()
        self.password = None

    def __on_mqtt_connect(self, client, userdata, flags, rc):
        _p = {
            "class_id": self.god.params.class_id,
            "uid": self.uid
        }
        self.topic_class = self.god.params.topic_class.format(**_p)
        self.topic_personal = self.god.params.topic_personal.format(**_p)
        client.subscribe(self.topic_class)
        client.subscribe(self.topic_personal)
        try:
            self.god.lock()
            self.god.on_teacher_ready(self)
            logger.info("Teacher [%s] ready." % self.identity)
        finally:
            self.god.unlock()

    def __on_mqtt_message(self, client, userdata, msg):
        try:
            self.god.on_teacher_msg(self, msg.topic, Message.FromString(msg.payload))
        except Exception as e:
            logger.error("Error while receiving MQTT message for teacher %s: %s" % (self.identity, str(e)))
            logger.error("%s -> %s" % (msg.topic, msg.payload))
        logger.debug("[Teacher: %s] %s -> %s" % (self.identity, msg.topic, str(msg.payload)))

    def __connect_mqtt(self):
        self.mqtt_client = mqtt.Client(self.identity)
        self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_token)
        self.mqtt_client.on_connect = self.__on_mqtt_connect
        self.mqtt_client.on_message = self.__on_mqtt_message
        self.mqtt_client.connect(self.god.params.mqtt_svr, self.god.params.mqtt_port, 60)

    def __login(self):
        url = "%s/mp-business/public/acl/teacher/login" % self.god.params.http_server
        data = {
            "loginName": self.identity,
            "password": self.password
        }
        headers = self._http_headers(data)
        resp = self.http_session.post(url, headers=headers, json=data)
        # logger.debug(resp.text)
        resp_json = json.loads(resp.text)
        if "success" in resp_json and resp_json['success']:
            self.uid = resp_json['data']['user']['teacherId']
            self.http_token = resp_json['data']['token']
            self.topic_personal = "person/u_%s" % self.uid
            logger.debug("Teacher login success: %s" % self.identity)
        else:
            raise LoginError('Failed login for teacher %s: %s' % (self.identity, resp_json['error']['message']))

    def __enter_classroom(self):
        url = "%s/mp-business/public/classroom/teacher/enter" % self.god.params.http_server
        data = {
            "teacherId": self.uid,
            "periodId": self.god.params.class_id
        }
        headers = self._http_headers(data)
        resp = self.http_session.get(url + "?" + urllib.parse.urlencode(data), headers=headers)
        resp_json = json.loads(resp.text)
        if resp_json['success']:
            self.mqtt_token = resp_json['data']['emqToken']
            self.mqtt_username = "pid_%s_vid_-1_uid_%s" % (self.god.params.class_id, self.uid)
        else:
            raise GeneralClassError('Failed on entering class with id: %s for teacher %s: %s' % (
                self.god.params.class_id, self.identity, resp_json['error']['message']))

    def _prepare(self):
        self.__login()
        self.__enter_classroom()
        self.__connect_mqtt()


class Student(Endpoint):
    def __init__(self, identity):
        super(Student, self).__init__(identity)
        self.http_session = requests.session()
        self.group_id = None

    def __on_mqtt_connect(self, client, userdata, flags, rc):
        _p = {
            "class_id": self.god.params.class_id,
            "group_id": self.group_id
        }
        self.topic_class = self.god.params.topic_class.format(**_p)
        self.topic_group = self.god.params.topic_group.format(**_p)
        client.subscribe(self.topic_class)
        client.subscribe(self.topic_group)
        client.subscribe(self.topic_personal)
        self.t31 = time.time()
        try:
            self.god.lock()
            logger.info("Student [%s] ready. (%.3fs, %.3fs, %.3fs, %.3fs)" % (
            self.identity, self.t11 - self.t10, self.t21 - self.t20, self.t31 - self.t30, time.time() - self.t0))
            self.god.on_student_ready(self)
        finally:
            self.god.unlock()

    def __on_mqtt_message(self, client, userdata, msg):
        logger.debug("[Student: %s] %s -> %s" % (self.identity, msg.topic, str(msg.payload)))
        try:
            self.god.on_student_msg(self, msg.topic, Message.FromString(msg.payload))
        except Exception as e:
            logger.error("Error while receiving MQTT message for student %s: %s" % (self.identity, str(e)))
            logger.error("%s -> %s" % (msg.topic, msg.payload))

    def __connect_mqtt(self):
        self.t30 = time.time()
        self.mqtt_client = mqtt.Client(self.identity)
        self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_token)
        self.mqtt_client.on_connect = self.__on_mqtt_connect
        self.mqtt_client.on_message = self.__on_mqtt_message
        self.mqtt_client.connect(self.god.params.mqtt_svr, self.god.params.mqtt_port, 60)

    def __login(self):
        self.t10 = time.time()
        url = "%s/mp-business/public/acl/student/sms" % self.god.params.http_server
        data = {
            "mobile": self.identity,
            "smsType": 1
        }
        headers = self._http_headers(data)
        resp = self.http_session.get(url + "?" + urllib.parse.urlencode(data), headers=headers)
        logger.debug("SMS: " + resp.text)

        _redis = redis.Redis(host=self.god.params.redis_host,
                             port=self.god.params.redis_port,
                             password=self.god.params.redis_password,
                             db=self.god.params.redis_db)
        _code = _redis.get("planet:auth:sms::1_2_1_1_%s" % self.identity)
        _code = _code.decode('utf8')[1:-1]

        url = "%s/mp-business/public/acl/student/mobile/login" % self.god.params.http_server
        data = {
            "mobile": self.identity,
            "verifyCode": _code
        }
        headers = self._http_headers(data)
        # print(headers)
        # print(data)
        resp = self.http_session.post(url, headers=headers, json=data)
        logger.debug("LOGIN: " + resp.text)
        resp_json = json.loads(resp.text)
        if resp_json['success']:
            self.uid = resp_json['data']['user']['id']
            self.http_token = resp_json['data']['token']
            self.topic_personal = "person/u_%s" % self.uid
            logger.debug("Student login success: %s [%.3fs]" % (self.identity, time.time() - self.t0))
        else:
            logger.error(resp.text)
            raise LoginError('Failed login for student %s' % self.identity)
        self.t11 = time.time()

    def __enter_classroom(self):
        self.t20 = time.time()
        url = "%s/mp-business/public/classroom/enter" % self.god.params.http_server
        data = {
            "studentId": self.uid,
            "periodId": self.god.params.class_id,
            # "p": '5'
        }
        headers = self._http_headers(data)
        resp = self.http_session.get(url + "?" + urllib.parse.urlencode(data), headers=headers)
        logger.debug("ENTER ROOM: " + resp.text)
        resp_json = json.loads(resp.text)
        if resp_json['success']:
            self.mqtt_token = resp_json['data']['emqToken']
            self.group_id = resp_json['data']['virtualClassId']
            self.mqtt_username = "pid_%s_vid_%s_uid_%s" % (self.god.params.class_id, self.group_id, self.uid)
        else:
            raise GeneralClassError('Failed to enter class with id: %s for student %s: %s' % (
                self.god.params.class_id, self.identity, resp_json['error']['message']))
        self.t21 = time.time()

    def _prepare(self):
        self.__login()
        self.__enter_classroom()
        self.__connect_mqtt()


class God:
    """
    God是上帝视角的管理类，具有如下功能：
    1. 感知老师和学生端点的重要行为（进入课堂/有消息到达/完成）
    2. 停止所有端点的处理循环
    """

    def __init__(self, params):
        self.teacher = None
        self.students = {}
        self.finished = threading.Event()
        self.__lock = threading.RLock()
        self.__params = params
        self.__ref_count = 0
        self._rs_lock = threading.RLock()

    def lock(self):
        self.__lock.acquire()

    def unlock(self):
        self.__lock.release()

    @property
    def params(self):
        return self.__params

    def stop(self):
        """
        停止所有端点的处理循环，需注意本方法会立即返回，但是所有端点真正处理完需要等待on_finish事件
        :return:
        """
        if self.teacher:
            self.teacher.stop()
        for stu in self.students.values():
            stu.stop()

    def create_teacher(self, identity, password):
        """
        创建老师端点，本方法只应该本调用一次，超过一次会触发DuplicatedTeacherError
        :param identity: 老师的登录名
        :param password: 老师的登录密码
        :return:
        """
        if self.teacher:
            raise DuplicatedTeacherError()

        self.__ref_count += 1
        teacher = Teacher(identity)
        teacher.password = password
        teacher.god = self
        teacher.start()
        self.teacher = teacher
        return teacher

    def create_student(self, identity):
        """
        创建一个学生端点，学生端点不需要提供密码，会自动使用手机验证码登录
        :param identity: 学生登录名
        :return:
        """
        try:
            self.lock()
            if identity in self.students:
                raise DuplicatedStudentError("Student %s already been created" % identity)

            self.__ref_count += 1
            student = Student(identity)
            self.students[identity] = student
            student.god = self
        finally:
            self.unlock()
        student.start()

        return student

    def on_teacher_msg(self, owner: Teacher, topic: str, data: Message):
        """
        老师端有信令消息到达，非线程安全
        :param owner: 老师端点实例
        :param topic: 收到消息的topic
        :param data: 信令
        :return:
        """
        raise NotImplementedError()

    def on_student_msg(self, owner: Student, topic: str, data: Message):
        """
        学生端有信令到达，非线程安全
        :param owner: 学生端点实例
        :param topic: 收到消息的topic
        :param data: 信令
        :return:
        """
        raise NotImplementedError()

    def on_teacher_ready(self, owner: Teacher):
        """
        老师端点成功进入课堂，线程安全
        :param owner: 老师端点实例
        :return:
        """
        raise NotImplementedError()

    def on_student_ready(self, owner: Student):
        """
        学生端点成功进入课堂，线程安全
        :param owner: 学生端点实例
        :return:
        """
        raise NotImplementedError()

    def on_finish(self):
        """
        所有端点完成事件，线程安全
        :return:
        """
        raise NotImplementedError()

    def endpoint_finish(self, instance):
        self.lock()
        self.__ref_count -= 1
        if self.__ref_count == 0:
            self.on_finish()
            self.finished.set()
        if isinstance(instance, Teacher):
            self.teacher = None
            logger.debug("Teacher %s finished." % instance.identity)
        elif instance.identity in self.students:
            del self.students[instance.identity]
            logger.debug("Student %s finished." % instance.identity)
        self.unlock()
