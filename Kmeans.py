import matplotlib.pyplot as plt
import pickle

file = open('cluster_result.txt')
line = file.readline()
cluster_result_str = line.split(',')
colordic = {}
colordic[0] = 'red'
colordic[1] = 'blue'
colordic[2] = 'green'
colordic[3] = 'black'
colordic[4] = 'purple'
x = []
y = []
color = []
# 从pickle文件读取降维结果
with open("usdata.pickle", "rb") as usdata:
    data = pickle.load(usdata)
    for i in range(10000):
        x.append(data[i][0])
        y.append(data[i][1])
        color.append(colordic[int(cluster_result_str[i])])
plt.scatter(x, y, c=color)
plt.show()
