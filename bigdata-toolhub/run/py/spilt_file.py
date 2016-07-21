# -*- coding:utf-8 -*-
import re
import os
import time
# str.split(string)分割字符串
#'连接符'.join(list) 将列表组成字符串


def splitFile(path, opath):
    global i

    if not os.path.isdir(path) and not os.path.isfile(path):
        return False

    if os.path.isfile(path):
        #print "path:", path
        #print "opath:", opath
        file_path = os.path.split(path)  # 分割出目录与文件
        lists = file_path[1].split('.')  # 分割出文件与文件扩展名
        file_ext = lists[-1]  # 取出后缀名(列表切片操作)
        file_name = lists[0]  # 取出后缀名(列表切片操作)
        #print "file_name:", file_name
        img_ext = ['txt']
        if file_ext in img_ext:
            i += 1  # 注意这里的i是一个陷阱
            err_file = opath + file_name + "_err." + file_ext
            right_file = opath + file_name + "_right." + file_ext
            #print "err_file:", err_file
            #print "right_file:", right_file
            split(path,err_file,right_file)

    elif os.path.isdir(path):
        #print '这是一个地址'
        for x in os.listdir(path):
            # os.path.join()在路径处理上很有用
            splitFile(os.path.join(path, x), opath)


def split(path, epath, rpath):
    #ifn = r"/usr/local/yinlian/python/data_card.txt"
    #ofn = r"/usr/local/yinlian/python_err/data_card.txt"
    #wfn = r"/usr/local/yinlian/python/data_card_right.txt"
    # 写入和写出带上“b”是为了防止读到二进制文件而无法读完的问题
    global j
    fileHandle = open(path, 'rb')

    fileList=fileHandle.readlines()
    line_list = []
    for k,line in enumerate(fileList) :
        #print k
        cnt = line.count("|")
        #print line.count("|")
        line_list.append(cnt)

    max_val = max(line_list)
    min_val = min(line_list)
    print "max value element : ", max(line_list)
    print "min value element : ", min(line_list)

    if max_val > min_val :
        print 'there is something wrong!'
    else :
        print 'everything is ok!'
    wrong_line = ''
    right_list = ''
    for id,val in enumerate(line_list) :
        #print id
        #print val
        if val == min_val :
            #print "right element id : ", id
            right_list += ''.join(fileList[id:id+1])
        else :
            #print "wrong element id : ", id
            wrong_line += ''.join(fileList[id:id+1])
    #print "wrong_line : ", wrong_line
    #print "right_list : ", right_list
    #print "wrong line len====",len(wrong_line)
    if len(wrong_line) > 0 :
        j += 1
        outfile = open(epath, 'wb')
        outfile.write(wrong_line)
        outfile.close()
    if len(right_list) > 0 :
        rightfile = open(rpath, 'wb')
        rightfile.write(right_list)
        fileHandle.close()
        rightfile.close()


data_dir = '/data1/python'
output_dir = '/data1/python_output/'

start = time.time()
i = 0
j = 0
splitFile(data_dir,output_dir)

c = time.time() - start
print('程序运行耗时:%0.2f' % (c))
print('总共处理了 %s 个文件' % (i))
print('其中有 %s 个包含错误数据的文件' % (j))
