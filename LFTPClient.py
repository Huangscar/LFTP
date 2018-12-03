# encoding:utf-8
import socket
import re
import os
import struct
import threading
import time
BUF_SIZE = 1500
FILE_BUF_SIZE = 1024
SERVER_PORT = 12000
CLIENT_FOLDER = 'ClientFiles/'   # 接收文件夹
WINDOW_SIZE = 1000
wsnd = WINDOW_SIZE
buffer_receive = []
threading_lock = threading.Lock()

# 传输文件时的数据包格式(序列号，确认号，文件结束标志，1024B的数据)
# pkt_value = (int seq, int ack, int end_flag 1024B的byte类型 data)
pkt_struct = struct.Struct('III1024s')


def lsend(client_socket, server_address, large_file_name):
    print("LFTP lsend", server_address, large_file_name)
    # 发送数据包次数计数
    pkt_count = 0
    # 模式rb 以二进制格式打开一个文件用于只读。文件指针将会放在文件的开头。
    file_to_send = open(CLIENT_FOLDER + large_file_name, 'rb')

    data_group = []
    while True:
        data_group.append(file_to_send.read(FILE_BUF_SIZE))
        if str(data_group[len(data_group) - 1]) == "b''":
            break

    # 发送ACK 注意要做好所有准备(比如打开文件)后才向服务端发送ACK
    client_socket.sendto('ACK'.encode('utf-8'), server_address)

    # 等待服务端的接收允许
    message, server_address = client_socket.recvfrom(BUF_SIZE)
    print('来自', server_address, '的数据是: ', message.decode('utf-8'))

    print('正在发送', large_file_name)
    # 用缓冲区循环发送数据包

    while True:
        # data = file_to_send.read(FILE_BUF_SIZE)

        send_package_num = 0

        #seq = pkt_count
        #ack = pkt_count

        # 将元组打包发送
        global is_exit
        is_exit = False
        is_end = False
        new_threading = threading.Thread(target=listen_package, args=(client_socket, 0))
        new_threading.start()
        for i in range(WINDOW_SIZE):

            # print(is_full)
            if is_full:
                # print(is_full)
                # print(i)
                break

            if str(data_group[pkt_count+i]) != "b''":  # b''表示文件读完
                end_flag = 0
                client_socket.sendto(pkt_struct.pack(*(pkt_count+i, int(pid), end_flag, data_group[pkt_count+i])), server_address)
                send_package_num += 1
            else:
                # print("end")
                is_end = True
                end_flag = 1  # 发送的结束标志为1，表示文件已发送完毕
                client_socket.sendto(pkt_struct.pack(*(pkt_count+i, int(pid), end_flag, 'end'.encode('utf-8'))), server_address)
                threading_lock.acquire()
                is_exit = True
                threading_lock.release()
                # print("end")
                # 等待ACK
                try:
                    new_threading.join()
                    ack_data_, server_address = client_socket.recvfrom(BUF_SIZE)
                    ack_num = int(ack_data_.decode('utf-8'))
                    pkt_count = ack_num-1
                    break
                except socket.timeout as e:
                    pkt_count = pkt_count+i
                    break
                except ConnectionResetError as e:
                    break

        if is_end:
            if pkt_count == len(data_group) - 1:
                break
            else:
                continue

        threading_lock.acquire()
        is_exit = True
        threading_lock.release()
        new_threading.join()

        # 等待服务端ACK,这里只会发送一个ACK，收到的ACK的值为需要的部分的开始
        try:
            ack_data_, server_address = client_socket.recvfrom(BUF_SIZE)
            while True:
                get_data = str(ack_data_.decode('utf-8'))
                if get_data.isdigit():
                    ack_num = int(get_data)
                    # print(ack_num)
                    pkt_count = ack_num
                    break
                else:
                    pkt_count = pkt_count
                    ack_data_, server_address = client_socket.recvfrom(BUF_SIZE)

        except socket.timeout as e:
            pkt_count = pkt_count
        except ConnectionResetError as e:
            break
    print(large_file_name, '发送完毕，发送数据包的数量：' + str(pkt_count))


def listen_package(client_socket, ack_type):
    global is_full
    is_full = False
    if ack_type == 0:
        # 循环接收，直到传输结束
        while not is_exit:
            try:
                ack_data_, server_address = client_socket.recvfrom(BUF_SIZE)
                if str(ack_data_.decode('utf-8')) == "isFull":
                    is_full = True
                break
            except socket.timeout as e:
                threading_lock.acquire()
                if is_exit:
                    threading_lock.release()
                    break
                else:
                    threading_lock.release()
            except ConnectionResetError as e:
                print(e)
                break


def lget(client_socket, server_address, large_file_name):
    print("LFTP lget", server_address, large_file_name)
    # 创建文件。模式wb 以二进制格式打开一个文件只用于写入。如果该文件已存在则打开文件，
    # 并从开头开始编辑，即原有内容会被删除。如果该文件不存在，创建新文件。
    file_to_recv = open(CLIENT_FOLDER + large_file_name, 'wb')
    # 接收数据包次数计数
    pkt_count = 0

    # 发送ACK 注意要做好所有准备(比如创建文件)后才向服务端发送ACK
    client_socket.sendto('ACK'.encode('utf-8'), server_address)
    need_ack = 0
    print('正在接收', large_file_name)
    # 开始接收数据包
    while True:
        # 用缓冲区接收数据包
        package_num = 0
        while len(buffer_receive) <= WINDOW_SIZE:
            try:
                # print(package_num)
                packed_data_, server_address_ = client_socket.recvfrom(BUF_SIZE)
                buffer_receive.append(packed_data_)
                package_num += 1
            except Exception as e:
                # print(e)
                break

        # 窗口满了，向发送端发送
        if package_num != 0:
            # print("full")
            client_socket.sendto('isFull'.encode('utf-8'), server_address_)

        # 从list里读包，是这个进程的包就写进去，不是就不管
        threading_lock.acquire()
        i = 0
        while i < len(buffer_receive):
            data_ = buffer_receive[i]
            unpacked_data = pkt_struct.unpack(data_)
            seq_num = unpacked_data[0]
            ack_num = unpacked_data[1]
            end_flag = unpacked_data[2]
            data = unpacked_data[3]
            buffer_receive.remove(data_)
            # print(seq_num)
            if seq_num == need_ack:
                if end_flag != 1:
                    file_to_recv.write(data)
                    need_ack += 1
                else:
                    break  # 结束标志为1,结束循环
        threading_lock.release()
        if package_num != 0:
            client_socket.sendto(str(need_ack).encode('utf-8'), server_address)
            # print(need_ack)
            if end_flag == 1:
                break
        else:
            client_socket.sendto(str(need_ack).encode('utf-8'), server_address)
        # print(len(buffer_receive))
        pkt_count += 1
        '''
        packed_data, server_address = client_socket.recvfrom(BUF_SIZE)
        # 解包，得到元组
        unpacked_data = pkt_struct.unpack(packed_data)
        end_flag = unpacked_data[2]
        data = unpacked_data[3]

        if end_flag != 1:
            file_to_recv.write(data)
        else:
            break  # 结束标志为1,结束循环
        # 向服务端发送ACK
        client_socket.sendto('ACK'.encode('utf-8'), server_address)
        pkt_count += 1
        '''

    file_to_recv.close()
    print('成功接收的数据包数量：' + str(need_ack))


def connection_request(client_socket, server_addr, cmd, large_file_name):
    # 若要发送的文件不存在，退出程序
    if cmd == 'lsend' and (os.path.exists(CLIENT_FOLDER + large_file_name) is False):
        print('要发送的文件不存在，退出程序')
        exit(2)

    # 三次握手
    # 连接请求，格式为[lsend|lget]#large_file_name，因此文件命名不允许含有#
    client_socket.sendto((cmd + '#' + large_file_name).encode('utf-8'), server_addr)
    # 接收连接允许报文
    while True:
        try:
            message, server_address = client_socket.recvfrom(BUF_SIZE)
            break
        except socket.timeout as e:
            continue
    global pid
    if len(message.decode('utf-8').split(',')) > 1:
        pid = message.decode('utf-8').split(',')[1]
    response = message.decode('utf-8').split(',')[0]
    print('来自', server_address, '的数据是: ', response)

    # 若服务端该文件不存在，退出程序
    if response == 'fileNotExists':
        exit(2)

    # 注意要做好所有准备(比如创建文件)后才向服务端发送ACK
    if cmd == 'lget':
        lget(client_socket, server_address, large_file_name)
    elif cmd == 'lsend':
        lsend(client_socket, server_address, large_file_name)


def read_command(client_socket):
    print('请输入命令: LFTP [lsend | lget] server_address large_file_name')
    pattern = re.compile(r"(LFTP) (lsend|lget) (\S+) (\S+)")
    # LFTP lget 127.0.0.1 CarlaBruni.mp3
    # LFTP lsend 127.0.0.1 CarlaBruni.mp3
    cmd = input()
    match = pattern.match(cmd)
    if match:
        cmd = match.group(2)
        server_ip = match.group(3)
        large_file_name = match.group(4)
        connection_request(client_socket, (server_ip, SERVER_PORT), cmd, large_file_name)
    else:
        print('[Error] Invalid command!')


def main():
    # 检查接收文件夹是否存在
    if os.path.exists(CLIENT_FOLDER) is False:
        print('创建文件夹', CLIENT_FOLDER)
        os.mkdir(CLIENT_FOLDER)

    # 创建客户端socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.settimeout(7)


    # 客户端输入命令
    read_command(client_socket)

    # 关闭客户端socket
    client_socket.close()


if __name__ == "__main__":
    main()
