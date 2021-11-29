#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :ftp_tool.py
@时间        :2021/06/22 11:57:44
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        : FTP 工具类，实现多文件下载与上传
'''

from ftplib import FTP
import os

class FTPTool(object):

    def __init__(self, host: str, port: int, username: str, password: str, log=None) -> None:
        """
        初始化方法，
        :param host : type str 主机IP
        :param port : type int ftp 端口号
        :param username : type str 用户名
        :param password : type str 密码
        :return None : 无返回值
        """
        super().__init__()
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = password
        self.__log = log
        self.__error = None
        # 获取主机 FTP 链接
        self.__ftp = self.__ftpconnect()

    def __show_logs(self, info):
        if self.__log is not None:
            self.__log.info(info)
        else:
            print(info)
        
    def __ftpconnect(self) -> FTP:
        """
        私有方法，连接ftp
        :return FTP : 返回登录后的FTP链接
        """
        try:
            ftp = FTP()
            # 打开调试级别2，显示详细信息
            ftp.set_debuglevel(0)
            # 链接
            ftp.connect(self.__host, self.__port)
            # 登录
            ftp.login(self.__username, self.__password)
            # 设置参数
            ftp.encoding='utf-8'
            self.__show_logs(ftp.welcome)
            return ftp
        except Exception as e:
            self.__error = e
            self.__show_logs(e)
            return None

    def __downloadfile(self, remotepath: str, localpath: str) -> int:
        """
        私有方法，从FTP主机下载文件
        :param remotepath : type str FTP 主机路径
        :param localpath : type str 本地路径
        :return int : 返回是否下载成功
        """

        # 设置的缓冲区大小
        bufsize = 1024
        try:
            with open(localpath, 'wb') as fp:
                self.__ftp.retrbinary('RETR %s'%remotepath, fp.write, bufsize)
                self.__ftp.set_debuglevel(0)# 参数为0，关闭调试模式
            return 1
        except Exception as e:
            self.__error = e
            self.__show_logs(e)
            return 0
        
    def __uploadfile(self, remotepath: str, localpath: str) -> int:
        """
        私有方法，从本地上传文件到FTP主机
        :param remotepath : type str FTP 主机路径
        :param localpath : type str 本地路径
        :return int : 返回是否上传成功
        """

        bufsize = 1024
        try:
            with open(localpath, 'rb') as fp:
                self.__ftp.storbinary('STOR %s'%remotepath, fp, bufsize)
                self.__ftp.set_debuglevel(0)# 参数为0，关闭调试模式
            return 1
        except Exception as e:
            self.__error = e
            self.__show_logs(e)
            return 0

    def __safe_mkdirs(self, dir: str, remote: bool=False) -> str:
        """
        私有方法，创建路径
        :param dir : type str 文件路径
        :param remote : type bool FTP主机或本地服务器路径
        :return str : 新建的路径
        """
        # 判断是否是 FTP主机更新路径
        if remote:
            try:
                parent_dir = os.path.dirname(dir)
                nlst_files = self.__ftp.nlst(parent_dir)
                # 检测 FTP 主机是否存在当前路径
                if dir not in nlst_files: 
                    self.__ftp.mkd(dir) # 创建路径
                return dir
            except Exception as e:
                self.__error = e
                self.__show_logs(e)
                return None
        else:
            # 判断本地服务器是否存在当前路径
            if not os.path.exists(dir):
                os.makedirs(dir) # 创建路径
            return dir
    
    def error(self) -> str:
        return str(self.__error)

    def check_conn(self) -> bool:
        return self.__ftp.sock is not None

    def check_path(self, remotepath) -> bool:
        nlst_files = self.__ftp.nlst(remotepath)
        return len(nlst_files) > 0

    def check_dirs(self, remotepath) -> bool:
        try:
            self.__ftp.cwd(remotepath)
            return True
        except:
            return False

    def downloaddirs(self, remotepath, localpath) -> int:
        """
        公共方法，从ftp迭代下载文件夹
        :param remotepath : type str FTP 主机目录
        :param localpath : type str 本地服务器路径
        :return int : 是否成功
        """
        remotepath = remotepath.rstrip("/")
        localpath = localpath.rstrip("/")
        try:
            # 新建本地路径，确保路径存在
            localpath = self.__safe_mkdirs(localpath)
            # 读取 FTP 文件列表
            nlst_files = self.__ftp.nlst(remotepath)
            self.__show_logs(str(remotepath))
            for f in nlst_files:
                current_path = "%s/%s" % (localpath, os.path.basename(f))
                if self.check_dirs(f):
                    # 遍历 FTP 文件目录
                    self.downloaddirs(f, current_path)
                else:
                    self.__downloadfile(f, current_path)
        except Exception as e:
            self.__error = e
            self.__show_logs(e)
            return 0

    def uploaddirs(self, remotepath, localpath) -> int:
        """
        公共方法，从本地迭代上传文件夹到ftp
        :param remotepath : type str FTP 主机目录
        :param localpath : type str 本地服务器路径
        :return int : 是否成功
        """

        remotepath = remotepath.rstrip("/")
        localpath = localpath.rstrip("/")

        try:
            # 新建服务器目录，确保路径存在
            remotepath = self.__safe_mkdirs(remotepath, True)
            if remotepath is None:
                return 0
        except Exception as e:
            self.__error = e
            self.__show_logs(e)
            return 0

        # 重定向服务器路径
        remotepath =  "%s/%s" % (remotepath, os.path.basename(localpath)) 
        # 避免路径中出现 \\ 不使用os.path.join
        # remotepath = os.path.join(remotepath, os.path.basename(localpath))

        if os.path.isfile(localpath):
            # 上传本地文件
            self.__uploadfile(remotepath, localpath)
        elif os.path.exists(localpath):
            lst_dirs = os.listdir(localpath)
            for f in lst_dirs:
                # 遍历文件夹，迭代上传文件
                self.uploaddirs(remotepath, "%s/%s" %(localpath, f))
        else:
            self.__show_logs("ERROR: %s is not exists !" % localpath)
            return 0
        
    def release(self) -> int:
        """
        公共方法，释放 FTP 链接资源
        :return int : 是否成功
        """
        try:
            if self.__ftp.sock is not None:
                # ftp 链接不为空时，释放链接
                self.__ftp.quit()
                self.__show_logs("释放 ftp 链接 ...")
            return 1
        except Exception as e:
            self.__error = e
            self.__show_logs(e)
            return 0

    def __del__(self):
        """
        析构函数，实例删除时，释放 FTP 链接资源
        """
        self.__show_logs("析构函数，释放资源")
        self.release()


if __name__ == '__main__':

    # ftp 工具调用样例说明

    # 初始化 FTP 实例，获取 FTP 链接
    ftp_tool = FTPTool("10.15.49.41", 22021, "ftpUser", "ftP!123")
    ftp_tool.error()
    # 从FTP服务器下载文件， 到指定目录
    ftp_tool.downloaddirs("/data/test/input/logAnomalyDetect/70d2529868f2469f83bdc2dcca40b9ae","/home/songy/weiqs_workspace/log_anomaly_detection/pkl_files")
    # 从本地上传指定文件到FTP目录
    # ftp_tool.uploaddirs("/data/input/faultearlywaring","/home/songy/weiqs_workspace/competition/p0_abnormal_predict/data/")
    #### 注意 ### FTP 链接使用完毕一定要释放资源。以下两种方式任选一种
    ftp_tool.release()
    # del ftp_tool