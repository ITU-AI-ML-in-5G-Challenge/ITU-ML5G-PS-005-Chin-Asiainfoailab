import traceback

import logging
import paramiko

import requests

logger = logging.getLogger(__name__)


class AgentObject(object):
    """
    接口代理的sftp上传下载功能
    """

    def __init__(self, agent_host, agent_port, agent_user=None, agent_password=None, local_path=None, sftp_path=None):
        """
        初始化
        :param agent_host: 主机名
        :param agent_port: 代理端口
        :param agent_user: 用户名
        :param agent_password: 密码
        :param local_path: 本地路径，即推理镜像中的地址，比如：D:/test.txt，这里必须具体到文件名称
        :param sftp_path: 远程路径，即集团训练镜像地址，比如：/home/test.txt
        """
        self.host = agent_host
        self.port = agent_port
        self.user = agent_user
        self.password = agent_password
        self.server_path = sftp_path
        self.local_path = local_path

    def restful_request(self, flag):
        """
        代理restful模型训练请求接口
        :param flag: 请求标识，"train"表示训练模型，"status"表示获取训练状态
        :return: 
        "train": {"code": 0,"data": "模型训练请求成功"}
        "status": {"code": 0,"data": {"status": 0},"msg": "模型状态查询请求成功"}
        失败情况下：{"code": 1, "data": "此请求不存在", "msg": "请求失败"}
        """
        if flag == "train":
            # 模型训练请求
            train_url = "http://" + self.host + ":" + str(self.port) + "/aiops/text_classification/train_model/"
            data = requests.get(train_url)
        elif flag == "status":
            # 模型状态查询
            status_url = "http://" + self.host + ":" + self.port + "/aiops/text_classification/train_status/"
            data = requests.get(status_url)
        else:
            data = {"code": 1, "data": "此请求不存在", "msg": "请求失败"}
        return data

    def sftp_upload_file(self, timeout=10):
        """
        上传文件，注意：不支持文件夹
        :param timeout: 超时时间(默认)，必须是int类型
        :return: bool
        """
        try:
            # # socket这种方式得再测试
            # my_socket = socket.socket()
            # my_socket.bind((self.host, self.port))
            # t = paramiko.Transport(my_socket)  # 这里使用socket对象
            t = paramiko.Transport((self.host, int(self.port)))  # 可连接
            t.banner_timeout = timeout
            t.connect(username=self.user, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(t)
            sftp.put(self.local_path, self.server_path)
            t.close()
            return True
        except Exception:
            logger.error(traceback.format_exc())
            return False

    def sftp_download_file(self, timeout=10):
        """
        下载文件，注意：不支持文件夹
        :param timeout: 超时时间(默认)，必须是int类型
        :return: bool
        """
        try:
            # # socket这种方式得再测试
            # my_socket = socket.socket()
            # my_socket.bind((self.host, self.port))
            # t = paramiko.Transport(my_socket)  # 这里使用socket对象
            t = paramiko.Transport((self.host, int(self.port)))  # 可连接
            t.banner_timeout = timeout
            t.connect(username=self.user, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(t)
            sftp.get(self.server_path, self.local_path)
            t.close()
            return True
        except Exception:
            logger.error(traceback.format_exc())
            return False


if __name__ == '__main__':

    agent_host = "10.15.49.41"  # 需要根据实际修改
    agent_port = 22022   # 需要根据实际修改
    agent_user = "puaiuc"     # 需要根据实际修改
    agent_password = "2021@4rtYU"    # 需要根据实际修改
    # local_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.sep + "sftp_dir"  # 这个路径需要根据实际修改
    # # print(local_dir)
    # if not os.path.exists(local_dir):
    #     os.mkdir(local_dir)
    # sftp_path = "/upload/test.csv"   # sftp路径，需要根据实际修改
    # local_path = local_dir + os.sep + "aa.csv"  # 本地路径，这里必须具体到文件名称，可根据实际修改
    local_path = "E:/data/aiops/model.json"
    sftp_path = "/data/input/model.json"
    sftp = AgentObject(agent_host, agent_port, agent_user, agent_password, local_path, sftp_path)
    # result = sftp.sftp_download_file()  # 下载
    result = sftp.sftp_upload_file() # 上传
    print(result)
